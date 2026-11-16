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

  The channel moves between `:unavailable`, `:idle`, `:leased`,
  `:awaiting_response`, and `:recovery_required`. Receive timeouts leave the
  exchange pending for the owner to finish or abort within the session deadline.
  I/O failures and sessions ending with an unfinished exchange require recovery.

  After explicitly recovering the modem, call `recover/1` to reopen the UART
  and discard stale input. Reopening alone does not cancel a modem operation.
  Nothing retries uncertain writes. Supervision restarts are not modem recovery.
  """
  @behaviour :gen_statem
  alias VintageNetECM.AT

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
  def session(channel, callback, opts) when is_function(callback, 1) do
    with {:ok, deadline} <- AT.deadline(opts),
         {:ok, handle} <- call(channel, {:session, deadline}, deadline) do
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
      data = %{
        tty: tty,
        ifname: Keyword.get(opts, :ifname),
        speed: speed,
        markers: markers,
        uart: nil,
        owner_monitor: if(opts[:owner], do: Process.monitor(opts[:owner])),
        session: nil
      }

      {:ok, :unavailable, data, [{:next_event, :internal, :open}]}
    else
      false -> {:stop, :invalid_options}
      {:error, reason} -> {:stop, reason}
    end
  end

  # Node-local tty ownership lasts until this process exits.
  defp claim_tty(tty) do
    if :global.set_lock({{__MODULE__, tty}, self()}, [node()], 0),
      do: :ok,
      else: {:error, :already_open}
  end

  @impl true
  def handle_event(:enter, old_state, state, data) do
    if old_state == state or state == :recovery_required or old_state == :recovery_required,
      do: publish_recovery(data, state == :recovery_required)

    :keep_state_and_data
  end

  def handle_event(type, :open, :unavailable, data) when type in [:internal, :state_timeout] do
    case open_uart(data) do
      {:ok, data} -> {:next_state, :idle, data}
      {:error, _, data} -> {:keep_state, data, [{:state_timeout, 5_000, :open}]}
    end
  end

  def handle_event(:info, {:EXIT, uart, _}, _state, %{uart: uart} = data),
    do: {:next_state, :recovery_required, %{data | uart: nil}}

  # A UART deliberately stopped during recovery may report its exit later.
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
      do: release(state, data)

  # A named timeout survives leased <-> awaiting_response transitions.
  def handle_event({:timeout, :session}, ref, state, %{session: %{ref: ref}} = data),
    do: release(state, data)

  def handle_event({:timeout, :session}, _, _state, _data), do: :keep_state_and_data

  def handle_event(
        {:call, {owner, _} = from},
        {:release, ref},
        state,
        %{session: %{ref: ref, owner: owner}} = data
      ),
      do: release(state, data, [{:reply, from, :ok}])

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

    if AT.remaining(deadline) == 0 do
      next =
        if state == :awaiting_response and AT.remaining(data.session.deadline) == 0,
          do: :recovery_required,
          else: state

      transition(next, data, from, {:error, :timeout})
    else
      perform(operation, deadline, from, state, data)
    end
  end

  def handle_event({:call, from}, {:exchange, _, _, _}, _state, _data),
    do: reply(from, {:error, :invalid_session})

  def handle_event({:call, {owner, _} = from}, {:session, deadline}, :idle, data) do
    if AT.remaining(deadline) == 0 or not Process.alive?(owner) do
      reply(from, {:error, :expired})
    else
      ref = make_ref()

      session = %{
        ref: ref,
        owner: owner,
        monitor: Process.monitor(owner),
        deadline: deadline,
        buffer: "",
        finals: nil
      }

      handle = %AT{channel: self(), ref: ref, deadline: deadline}

      {:next_state, :leased, %{data | session: session},
       [{:reply, from, {:ok, handle}}, {{:timeout, :session}, AT.remaining(deadline), ref}]}
    end
  end

  def handle_event({:call, from}, {:session, _}, _state, _data),
    do: reply(from, {:error, :busy})

  defp perform({:send, bytes}, deadline, from, _state, data) do
    case AT.validate_bytes(bytes) do
      :ok ->
        case write(data.uart, bytes, deadline) do
          :ok -> transition(:awaiting_response, data, from, :ok)
          error -> transition(:recovery_required, data, from, error)
        end

      error ->
        reply(from, error)
    end
  end

  defp perform(:receive, deadline, from, state, data),
    do: perform({:receive, data.session.finals}, deadline, from, state, data)

  defp perform({:receive, finals}, deadline, from, _state, data) do
    case read_response(data.uart, data.session.buffer, data.markers, deadline, finals) do
      {:ok, event, rest} ->
        next = if elem(event, 0) == :response, do: :leased, else: :awaiting_response

        session = %{
          data.session
          | buffer: rest,
            finals: if(next == :awaiting_response, do: finals)
        }

        transition(next, %{data | session: session}, from, {:ok, event})

      {:error, :timeout, buffer} ->
        session = %{data.session | buffer: buffer, finals: finals}

        next =
          if AT.remaining(session.deadline) == 0, do: :recovery_required, else: :awaiting_response

        transition(next, %{data | session: session}, from, {:error, :timeout})

      {:error, reason, _} ->
        transition(:recovery_required, data, from, {:error, reason})
    end
  end

  defp perform(:drain, _deadline, from, :awaiting_response, _data),
    do: reply(from, {:error, :exchange_pending})

  defp perform(:drain, _deadline, from, :leased, data) do
    case io(fn -> Circuits.UART.read(data.uart, 0) end) do
      {:ok, _} -> transition(:leased, put_in(data.session.buffer, ""), from, :ok)
      error -> transition(:recovery_required, data, from, error)
    end
  end

  defp reply(from, result), do: {:keep_state_and_data, [{:reply, from, result}]}

  defp transition(state, data, from, result),
    do: {:next_state, state, data, [{:reply, from, result}]}

  defp release(state, data, actions \\ []) do
    Process.demonitor(data.session.monitor, [:flush])
    next = if state == :leased, do: :idle, else: :recovery_required
    {:next_state, next, %{data | session: nil}, [{{:timeout, :session}, :cancel} | actions]}
  end

  defp reopen(from, data) do
    close_uart(data.uart)

    case open_uart(%{data | uart: nil}) do
      {:ok, data} -> transition(:idle, data, from, :ok)
      {:error, reason, data} -> transition(:recovery_required, data, from, {:error, reason})
    end
  end

  defp publish_recovery(%{ifname: nil}, _required), do: :ok

  defp publish_recovery(data, required) do
    PropertyTable.put(
      VintageNet,
      ["interface", data.ifname, "mobile", "recovery_required"],
      required
    )
  end

  defp read_response(uart, buffer, markers, deadline, finals) do
    parsed =
      if finals,
        do: AT.parse_response(buffer, markers, finals),
        else: AT.parse_response(buffer, markers)

    with true <- AT.remaining(deadline) > 0,
         :more <- parsed,
         {:ok, bytes} when bytes != "" <-
           io(fn -> Circuits.UART.read(uart, AT.remaining(deadline)) end) do
      read_response(uart, buffer <> bytes, markers, deadline, finals)
    else
      false -> {:error, :timeout, buffer}
      {:ok, ""} -> {:error, :timeout, buffer}
      {:error, reason} -> {:error, reason, buffer}
      result -> result
    end
  end

  defp open_uart(data) do
    case Circuits.UART.start_link() do
      {:ok, uart} -> configure_uart(uart, data)
      {:error, reason} -> {:error, reason, data}
    end
  end

  defp configure_uart(uart, data) do
    with :ok <- io(fn -> Circuits.UART.open(uart, data.tty, speed: data.speed, active: false) end),
         :ok <- io(fn -> Circuits.UART.flush(uart, :receive) end) do
      {:ok, %{data | uart: uart}}
    else
      {:error, reason} ->
        close_uart(uart)
        {:error, reason, data}
    end
  end

  defp write(uart, bytes, deadline) do
    case AT.remaining(deadline) do
      0 -> {:error, :timeout}
      timeout -> io(fn -> Circuits.UART.write(uart, bytes, timeout) end)
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
