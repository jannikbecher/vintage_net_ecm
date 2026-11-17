# SPDX-FileCopyrightText: 2026 Ben Youngblood
#
# SPDX-License-Identifier: Apache-2.0

defmodule VintageNetECM.AT.Channel do
  @moduledoc """
  A supervised, exclusive AT channel. No radio configuration or network polling.

  Add `{VintageNetECM.AT.Channel, tty: "/dev/ttyUSB2", name: MyApp.Modem}` to your
  supervision tree, then use `VintageNetECM.AT.session/3` with its name or PID.
  Options are `:tty`, `:name`, `:speed` (115,200), and `:intermediate_responses` (`[]`).
  An optional `:owner` PID bounds the lifetime of a temporary, unlinked channel.
  Managed channels use `:ifname` to publish `mobile.recovery_required` in VintageNet.
  Tty paths are claimed once per node. Do not use aliases for an already-owned tty.

  UART input is asynchronous. A receive waits only its caller, not the channel:
  competing sessions return `:busy` while an exchange is in progress.
  Receive timeouts preserve input and ownership so the owner can finish or abort
  within the session deadline. A final response finishes an exchange, not its
  session. Sessions can contain arbitrarily many sends and receives.

  Application I/O failures and sessions ending with unfinished exchanges require
  recovery. After explicitly restoring the modem, call `recover/1` to reopen the
  UART and discard stale input. Reopening alone does not cancel a modem operation.
  Nothing retries uncertain application writes.

  Controller-owned failures are different: the channel consumes a late final
  reply, or otherwise stops so its supervisor restarts it with a fresh UART.
  """
  @behaviour :gen_statem
  alias VintageNetECM.AT

  @max_buffer 16_384
  @late_reasons [:timeout, :unfinished_exchange, :session_expired, :owner_down]

  defstruct [
    :tty,
    :ifname,
    :speed,
    :markers,
    :late_reply_timeout,
    :uart,
    :session,
    :reader,
    :finals,
    :failure,
    :owner_monitor,
    buffer: ""
  ]

  def child_spec(opts), do: %{id: __MODULE__, start: {__MODULE__, :start_link, [opts]}}
  def start_link(opts), do: start(:start_link, opts)

  @doc false
  def start(opts), do: start(:start, opts)

  defp start(function, opts) do
    case Keyword.get(opts, :name) do
      nil ->
        apply(:gen_statem, function, [__MODULE__, opts, []])

      name when is_atom(name) ->
        apply(:gen_statem, function, [{:local, name}, __MODULE__, opts, []])

      name ->
        apply(:gen_statem, function, [name, __MODULE__, opts, []])
    end
  end

  @doc "Reopen a fenced channel only after the modem has been explicitly recovered."
  def recover(channel) do
    deadline = System.monotonic_time(:millisecond) + 5_000
    call(channel, {:recover, deadline}, deadline)
  end

  @doc false
  def controller_session(channel, callback, opts),
    do: session(channel, callback, opts, :controller)

  @doc false
  def session(channel, callback, opts), do: session(channel, callback, opts, :application)

  defp session(channel, callback, opts, kind) when is_function(callback, 1) do
    with {:ok, deadline} <- AT.deadline(opts),
         {:ok, handle} <- call(channel, {:session, deadline, kind}, deadline) do
      try do
        callback.(handle)
      after
        call(handle.channel, {:release, handle.ref}, deadline)
      end
    end
  end

  @doc false
  def exchange(%AT{} = handle, operation, opts) do
    with {:ok, deadline} <- AT.deadline(opts, handle.deadline) do
      call(handle.channel, {:exchange, handle.ref, operation, deadline}, deadline)
    end
  end

  defp call(channel, request, deadline) do
    :gen_statem.call(channel, request, AT.remaining(deadline) + 1_000)
  catch
    :exit, {:noproc, _} -> {:error, :not_running}
    :exit, _ -> {:error, :outcome_unknown}
  end

  @impl true
  def callback_mode, do: [:handle_event_function, :state_enter]

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)
    tty = Path.expand(Keyword.fetch!(opts, :tty), "/dev")
    markers = Keyword.get(opts, :intermediate_responses, [])
    speed = Keyword.get(opts, :speed, 115_200)

    with :ok <- AT.validate_intermediate_responses(markers),
         true <- is_integer(speed) and speed > 0,
         :ok <- claim_tty(tty) do
      data = %__MODULE__{
        tty: tty,
        ifname: Keyword.get(opts, :ifname),
        speed: speed,
        markers: markers,
        late_reply_timeout: Keyword.get(opts, :late_reply_timeout, 30_000),
        owner_monitor: if(opts[:owner], do: Process.monitor(opts[:owner]))
      }

      # Clear any flag left by a crashed predecessor before start_link returns,
      # so a restarted controller never reads a stale `true`.
      publish_recovery(data, false)
      {:ok, :unavailable, data, [{:next_event, :internal, :open}]}
    else
      false -> {:stop, :invalid_options}
      {:error, reason} -> {:stop, reason}
    end
  end

  defp claim_tty(tty) do
    if :global.set_lock({{__MODULE__, tty}, self()}, [node()], 0),
      do: :ok,
      else: {:error, :already_open}
  end

  @impl true
  def handle_event(:enter, old, state, data) do
    if old == state or :recovery_required in [old, state],
      do: publish_recovery(data, state == :recovery_required)

    # Entered once per failure, so the grace period cannot be extended.
    if state == :recovery_required and data.failure.late_reply,
      do: {:keep_state_and_data, [{:state_timeout, data.late_reply_timeout, :late_reply}]},
      else: :keep_state_and_data
  end

  def handle_event(type, :open, :unavailable, data) when type in [:internal, :state_timeout] do
    case open_uart(data) do
      {:ok, data} -> {:next_state, :idle, data}
      {:error, _, data} -> {:keep_state, data, [{:state_timeout, 5_000, :open}]}
    end
  end

  # PID-tagged input makes notifications from an old UART harmless after recovery.
  def handle_event(:info, {:circuits_uart, uart, {:error, reason}}, _state, %{uart: uart} = data),
    do: fence(data, {:io, reason})

  def handle_event(:info, {:circuits_uart, uart, bytes}, state, %{uart: uart} = data)
      when is_binary(bytes) do
    buffer = data.buffer <> bytes

    cond do
      state in [:idle, :unavailable] ->
        :keep_state_and_data

      state == :recovery_required and not data.failure.late_reply ->
        :keep_state_and_data

      byte_size(buffer) > @max_buffer ->
        fence(%{data | buffer: ""}, :response_too_large)

      state == :recovery_required ->
        {:keep_state, %{data | buffer: buffer}, [{:next_event, :internal, :late_reply}]}

      true ->
        receive_reply(%{data | buffer: buffer})
    end
  end

  def handle_event(:info, {:circuits_uart, _, _}, _state, _data), do: :keep_state_and_data

  def handle_event(:info, {:EXIT, uart, _}, _state, %{uart: uart} = data),
    do: fence(%{data | uart: nil}, :io_error)

  def handle_event(:info, {:EXIT, _, _}, _state, _data), do: :keep_state_and_data

  def handle_event(
        :info,
        {:DOWN, monitor, :process, _, _},
        _state,
        %{owner_monitor: monitor} = data
      ),
      do: {:stop, :normal, data}

  def handle_event(
        :info,
        {:DOWN, monitor, :process, _, _},
        state,
        %{session: %{monitor: monitor}} = data
      ),
      do: release(state, data, :owner_down)

  def handle_event({:timeout, :session}, ref, state, %{session: %{ref: ref}} = data),
    do: release(state, data, :session_expired)

  def handle_event({:timeout, :receive}, ref, _state, %{reader: %{ref: ref}} = data),
    do: receive_timeout(data)

  def handle_event({:timeout, _}, _, _state, _data), do: :keep_state_and_data

  # A controller command that missed its deadline may still finish. Consuming its
  # final response restores synchronization without probes, resets, or replays.
  def handle_event(
        :internal,
        :late_reply,
        :recovery_required,
        %{session: nil, failure: %{late_reply: true}} = data
      ) do
    case parse(data) do
      {:ok, {:response, _}, _} -> {:next_state, :idle, clear_exchange(data)}
      :more -> :keep_state_and_data
      {:error, reason} -> {:stop, {:controller_failure, reason}, data}
    end
  end

  def handle_event(:internal, :late_reply, _state, _data), do: :keep_state_and_data

  def handle_event(:state_timeout, :late_reply, :recovery_required, data),
    do: {:stop, {:controller_failure, :no_final_response}, data}

  def handle_event(
        {:call, {owner, _} = from},
        {:release, ref},
        state,
        %{session: %{ref: ref, owner: owner}} = data
      ),
      do: release(state, data, :unfinished_exchange, [{:reply, from, :ok}])

  def handle_event({:call, from}, {:release, _}, _state, _data),
    do: reply(from, {:error, :invalid_session})

  def handle_event({:call, from}, {:recover, deadline}, _state, data) do
    cond do
      AT.remaining(deadline) == 0 -> reply(from, {:error, :expired})
      data.session != nil -> reply(from, {:error, :busy})
      true -> reopen(from, data)
    end
  end

  def handle_event({:call, from}, _request, :recovery_required, _data),
    do: reply(from, {:error, :desynchronized})

  def handle_event({:call, from}, _request, :unavailable, _data),
    do: reply(from, {:error, :tty_not_open})

  def handle_event(
        {:call, {owner, _} = from},
        {:exchange, ref, operation, deadline},
        state,
        %{session: %{ref: ref, owner: owner}} = data
      ) do
    deadline = min(deadline, data.session.deadline)

    if AT.remaining(deadline) == 0,
      do: reply(from, {:error, :timeout}),
      else: perform(operation, deadline, from, state, data)
  end

  def handle_event({:call, from}, {:exchange, _, _, _}, _state, _data),
    do: reply(from, {:error, :invalid_session})

  def handle_event({:call, {owner, _} = from}, {:session, deadline, kind}, :idle, data) do
    if AT.remaining(deadline) == 0 or not Process.alive?(owner) do
      reply(from, {:error, :expired})
    else
      ref = make_ref()

      session = %{
        ref: ref,
        owner: owner,
        kind: kind,
        monitor: Process.monitor(owner),
        deadline: deadline
      }

      handle = %AT{channel: self(), ref: ref, deadline: deadline}

      {:next_state, :leased, %{data | session: session},
       [{:reply, from, {:ok, handle}}, {{:timeout, :session}, AT.remaining(deadline), ref}]}
    end
  end

  def handle_event({:call, from}, {:session, _, _}, _state, _data),
    do: reply(from, {:error, :busy})

  defp perform({:send, bytes}, deadline, from, _state, data) do
    case AT.validate_bytes(bytes) do
      :ok ->
        write(data, bytes, deadline, from)

      error ->
        reply(from, error)
    end
  end

  defp perform(:receive, deadline, from, _state, data) do
    ref = make_ref()
    data = %{data | reader: %{from: from, deadline: deadline, ref: ref}}
    receive_reply(data, [{{:timeout, :receive}, AT.remaining(deadline), ref}])
  end

  defp perform({:begin_command, _}, _deadline, from, :awaiting_response, _data),
    do: reply(from, {:error, :exchange_pending})

  defp perform({:begin_command, finals}, _deadline, from, :leased, data) do
    case io(fn -> Circuits.UART.flush(data.uart, :receive) end) do
      :ok ->
        transition(:leased, %{data | buffer: "", finals: finals}, from, :ok)

      {:error, reason} ->
        fence(data, {:io, reason}, [{:reply, from, {:error, reason}}])
    end
  end

  defp write(data, bytes, deadline, from) do
    case io(fn -> Circuits.UART.write(data.uart, bytes, AT.remaining(deadline)) end) do
      :ok -> transition(:awaiting_response, data, from, :ok)
      {:error, reason} -> fence(data, {:write, reason}, [{:reply, from, {:error, reason}}])
    end
  end

  defp receive_reply(data, actions \\ [])
  defp receive_reply(%{reader: nil} = data, actions), do: {:keep_state, data, actions}

  defp receive_reply(data, actions) do
    if AT.remaining(data.reader.deadline) == 0 do
      receive_timeout(data)
    else
      receive_parsed(parse(data), data, actions)
    end
  end

  defp receive_parsed(:more, data, actions), do: {:next_state, :awaiting_response, data, actions}

  defp receive_parsed({:ok, event, rest}, data, _actions) do
    next = if elem(event, 0) == :response, do: :leased, else: :awaiting_response
    replies = reader_reply(data, {:ok, event})
    data = %{data | reader: nil, buffer: rest}
    data = if next == :leased, do: %{data | finals: nil}, else: data
    {:next_state, next, data, replies}
  end

  defp receive_parsed({:error, reason}, data, _actions), do: fence(data, reason)

  defp receive_timeout(data) do
    actions = reader_reply(data, {:error, :timeout})
    data = %{data | reader: nil}

    if AT.remaining(data.session.deadline) == 0,
      do: fence(data, :timeout, actions),
      else: {:next_state, :awaiting_response, data, actions}
  end

  defp reader_reply(%{reader: nil}, _result), do: []

  defp reader_reply(data, result),
    do: [{{:timeout, :receive}, :cancel}, {:reply, data.reader.from, result}]

  defp reply(from, result), do: {:keep_state_and_data, [{:reply, from, result}]}

  defp transition(state, data, from, result),
    do: {:next_state, state, data, [{:reply, from, result}]}

  defp release(state, data, reason, actions \\ []) do
    Process.demonitor(data.session.monitor, [:flush])

    actions = [
      {{:timeout, :session}, :cancel} | reader_reply(data, {:error, :timeout}) ++ actions
    ]

    if state == :leased do
      {:next_state, :idle, clear_exchange(%{data | session: nil}), actions}
    else
      # Record the owner before dropping the lease. Recovery must never cross
      # from an application's unfinished conversation into controller work.
      data = record_failure(data, reason)

      fenced(%{data | session: nil}, reason, actions ++ [{:next_event, :internal, :late_reply}])
    end
  end

  defp fence(data, reason, actions \\ []) do
    data = record_failure(data, reason)
    fenced(data, reason, reader_reply(data, {:error, reason}) ++ actions)
  end

  # Application failures stay fenced until the application recovers the modem.
  # Controller commands are fixed and repeatable: wait for a late final reply,
  # otherwise let it crash. The supervisor reopens the UART and the controller
  # reinitializes when it sees the channel go down.
  defp fenced(%{failure: %{owner: :controller, late_reply: false}} = data, reason, actions),
    do: {:stop_and_reply, {:controller_failure, reason}, replies(actions), data}

  defp fenced(data, _reason, actions),
    do: {:next_state, :recovery_required, %{data | reader: nil}, actions}

  defp replies(actions), do: for({:reply, _, _} = reply <- actions, do: reply)

  # The first failure decides ownership; any later non-timeout failure rules out
  # a late reply.
  defp record_failure(data, reason) do
    owner = failure_owner(data)
    late_reply? = owner == :controller and data.finals != nil and reason in @late_reasons
    failure = data.failure || %{owner: owner, late_reply: late_reply?}
    %{data | failure: %{failure | late_reply: failure.late_reply and late_reply?}}
  end

  defp failure_owner(%{session: %{kind: kind}}), do: kind
  defp failure_owner(%{ifname: nil}), do: :application
  defp failure_owner(_data), do: :controller

  defp clear_exchange(data),
    do: %{data | buffer: "", reader: nil, finals: nil, failure: nil}

  defp parse(data) do
    if data.finals,
      do: AT.parse_response(data.buffer, data.markers, data.finals),
      else: AT.parse_response(data.buffer, data.markers)
  end

  defp reopen(from, data) do
    close_uart(data.uart)

    case open_uart(%{data | uart: nil}) do
      {:ok, data} -> transition(:idle, clear_exchange(data), from, :ok)
      {:error, reason, data} -> fence(data, {:io, reason}, [{:reply, from, {:error, reason}}])
    end
  end

  defp publish_recovery(%{ifname: nil}, _required), do: :ok

  defp publish_recovery(data, required) do
    prefix = ["interface", data.ifname, "mobile"]
    owner = if required, do: data.failure.owner

    PropertyTable.put_many(VintageNet, [
      {prefix ++ ["recovery_owner"], owner},
      {prefix ++ ["recovery_required"], required}
    ])
  end

  defp open_uart(data) do
    case Circuits.UART.start_link() do
      {:ok, uart} -> configure_uart(uart, data)
      {:error, reason} -> {:error, reason, data}
    end
  end

  defp configure_uart(uart, data) do
    with :ok <-
           io(fn ->
             Circuits.UART.open(uart, data.tty, speed: data.speed, active: false, id: :pid)
           end),
         :ok <- io(fn -> Circuits.UART.flush(uart, :receive) end),
         :ok <- io(fn -> Circuits.UART.configure(uart, active: true) end) do
      {:ok, %{data | uart: uart}}
    else
      {:error, reason} ->
        close_uart(uart)
        {:error, reason, data}
    end
  end

  defp io(fun) do
    fun.()
  catch
    :exit, _ -> {:error, :io_error}
  end

  defp close_uart(nil), do: :ok

  defp close_uart(uart) do
    _ = io(fn -> Circuits.UART.close(uart) end)
    _ = io(fn -> Circuits.UART.stop(uart) end)
    :ok
  end

  @impl true
  def terminate(_reason, _state, data), do: close_uart(data.uart)

  @impl true
  def format_status(status) do
    Map.new(status, fn
      {:log, _} ->
        {:log, []}

      {key, _} when key in [:state, :data, :message, :reason, :queue, :postponed, :timeouts] ->
        {key, :redacted}

      entry ->
        entry
    end)
  end
end
