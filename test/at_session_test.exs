# SPDX-FileCopyrightText: 2026 Ben Youngblood
#
# SPDX-License-Identifier: Apache-2.0

defmodule VintageNetECM.ATSessionTest do
  use ExUnit.Case, async: true
  alias VintageNetECM.AT
  alias VintageNetECM.AT.Channel
  alias VintageNetECM.TestPTY, as: PTY

  setup do
    pty = start_supervised!({PTY, []})
    channel = start_supervised!({Channel, tty: PTY.tty(pty), intermediate_responses: ["> "]})
    assert {:idle, _} = :sys.get_state(channel)
    %{pty: pty, channel: channel}
  end

  test "state transitions distinguish a lease from an unfinished exchange", %{
    channel: channel,
    pty: pty
  } do
    assert {:idle, %{session: nil}} = :sys.get_state(channel)

    AT.session(channel, fn at ->
      assert {:leased, %{session: %{owner: owner}}} = :sys.get_state(channel)
      assert owner == self()
      assert AT.send(at, "AT+X\r") == :ok
      assert {:awaiting_response, _} = :sys.get_state(channel)
      assert PTY.read(pty, 5) == "AT+X\r"
      :ok = PTY.write(pty, "> ")
      assert AT.receive(at) == {:ok, {:intermediate, "> "}}
      assert {:awaiting_response, _} = :sys.get_state(channel)
      assert AT.receive(at, timeout: 20) == {:error, :timeout}
      assert {:awaiting_response, _} = :sys.get_state(channel)
      assert AT.send(at, <<27>>) == :ok
      assert PTY.read(pty, 1) == <<27>>
      :ok = PTY.write(pty, "ERROR\r\n")
      assert AT.receive(at) == {:ok, {:response, ["ERROR"]}}
      assert {:leased, _} = :sys.get_state(channel)
    end)

    assert {:idle, %{session: nil}} = :sys.get_state(channel)
  end

  test "session expiry survives exchange state changes and release cancels the old timeout", %{
    channel: channel,
    pty: pty
  } do
    AT.session(
      channel,
      fn at ->
        assert AT.send(at, "AT\r") == :ok
        assert PTY.read(pty, 3) == "AT\r"
        :ok = PTY.write(pty, "OK\r\n")
        assert AT.receive(at) == {:ok, {:response, ["OK"]}}
        # Leased -> awaiting_response -> leased must not cancel the lease timer.
        Process.sleep(120)
        assert {:idle, %{session: nil}} = :sys.get_state(channel)
      end,
      timeout: 100
    )

    AT.session(channel, fn _ -> :ok end, timeout: 20)

    AT.session(
      channel,
      fn _ ->
        Process.sleep(40)
        assert {:leased, _} = :sys.get_state(channel)
      end,
      timeout: 1_000
    )

    assert {:idle, %{session: nil}} = :sys.get_state(channel)
  end

  test "an unavailable tty retries opening without creating a session", %{pty: pty} do
    tty = Path.join(System.tmp_dir!(), "ecm-late-#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm(tty) end)
    :ok = stop_supervised(Channel)
    channel = start_supervised!({Channel, tty: tty})
    assert {:unavailable, %{session: nil}} = :sys.get_state(channel)
    assert AT.session(channel, fn _ -> flunk("unavailable") end) == {:error, :tty_not_open}
    File.ln_s!(PTY.tty(pty), tty)
    # Exercise the retry with the real state timeout, without issuing recovery.
    Process.sleep(5_100)
    assert {:idle, %{session: nil}} = :sys.get_state(channel)
    assert AT.session(channel, fn _ -> :opened end) == :opened
  end

  test "callback results, exceptions, PID ownership, stale handles and competing sessions", %{
    channel: channel
  } do
    handle =
      AT.session(channel, fn at ->
        task = Task.async(fn -> AT.send(at, "foreign") end)
        assert Task.await(task) == {:error, :invalid_session}
        assert AT.session(channel, fn _ -> flunk("busy") end) == {:error, :busy}
        at
      end)

    assert AT.send(handle, "stale") == {:error, :invalid_session}

    assert_raise RuntimeError, "callback failed", fn ->
      AT.session(channel, fn _ -> raise "callback failed" end)
    end

    assert AT.session(channel, fn _ -> :application_result end) == :application_result
  end

  test "fragmented prompts and responses retain interleaved unsolicited lines", %{
    pty: pty,
    channel: channel
  } do
    task =
      Task.async(fn ->
        AT.session(channel, fn at ->
          assert AT.send(at, "AT+X\r") == :ok
          assert AT.receive(at) == {:ok, {:intermediate, "> "}}
          assert AT.send(at, "payload" <> <<26>>) == :ok
          AT.receive(at)
        end)
      end)

    assert PTY.read(pty, 5) == "AT+X\r"

    for fragment <- ["\r", "\n>", " "] do
      assert PTY.write(pty, fragment) == :ok
      Process.sleep(5)
    end

    assert PTY.read(pty, 8) == "payload" <> <<26>>

    for fragment <- ["+NOTICE: 1\r\n+X:", " 7\r\nO", "K\r\n"] do
      assert PTY.write(pty, fragment) == :ok
      Process.sleep(5)
    end

    assert Task.await(task) == {:ok, {:response, ["+NOTICE: 1", "+X: 7", "OK"]}}
    assert AT.session(channel, fn _ -> :clean end) == :clean
  end

  test "standard commands strip echo and keep extended errors synchronized", %{
    pty: pty,
    channel: channel
  } do
    for {response, result} <- [
          {"AT\r\nvalue\r\nOK\r\n", {:ok, ["value"]}},
          {"+CME ERROR: 30\r\n", {:error, {:cme, "30"}}}
        ] do
      task = Task.async(fn -> AT.session(channel, &AT.command(&1, "AT")) end)
      assert PTY.read(pty, 3) == "AT\r"
      assert PTY.write(pty, response) == :ok
      assert Task.await(task) == result
    end
  end

  test "an expired operation deadline does not fence a still-live session", %{
    channel: channel,
    pty: pty
  } do
    AT.session(channel, fn at ->
      assert AT.send(at, "AT\r") == :ok
      assert PTY.read(pty, 3) == "AT\r"
      expired = %{at | deadline: System.monotonic_time(:millisecond) - 1}
      assert AT.receive(expired) == {:error, :timeout}
      refute elem(:sys.get_state(channel), 0) == :recovery_required
      assert AT.command(at, "AT") == {:error, :exchange_pending}
      assert PTY.write(pty, "OK\r\n") == :ok
      assert AT.receive(at) == {:ok, {:response, ["OK"]}}
    end)

    assert AT.session(channel, fn _ -> :clean end) == :clean
  end

  test "response timeout obeys the session deadline and recovery never replays writes", %{
    channel: channel,
    pty: pty
  } do
    started = System.monotonic_time(:millisecond)

    assert AT.session(
             channel,
             fn at ->
               assert AT.send(at, "AT+SLOW\r") == :ok
               AT.receive(at, timeout: 120_000)
             end,
             timeout: 40
           ) == {:error, :timeout}

    assert System.monotonic_time(:millisecond) - started < 1_000
    assert PTY.read(pty, 8) == "AT+SLOW\r"
    assert AT.session(channel, fn _ -> flunk("fenced") end) == {:error, :desynchronized}

    # The simulated modem has finished its old operation before explicit recovery.
    assert PTY.write(pty, "OK\r\n") == :ok
    assert Channel.recover(channel) == :ok
    task = Task.async(fn -> AT.session(channel, &AT.command(&1, "AT")) end)
    assert PTY.read(pty, 3) == "AT\r"
    assert PTY.write(pty, "OK\r\n") == :ok
    assert Task.await(task) == {:ok, []}
  end

  test "caller death with an incomplete exchange fences the channel", %{
    channel: channel,
    pty: pty
  } do
    parent = self()

    {pid, monitor} =
      spawn_monitor(fn ->
        AT.session(channel, fn at ->
          :ok = AT.send(at, "AT+X\r")
          send(parent, :sent)

          receive do
            :never -> :ok
          end
        end)
      end)

    assert_receive :sent
    assert PTY.read(pty, 5) == "AT+X\r"
    Process.exit(pid, :kill)
    assert_receive {:DOWN, ^monitor, :process, ^pid, :killed}
    # This queued request is handled after the channel's owner monitor notification.
    assert AT.session(channel, fn _ -> flunk("must not run") end) in [
             {:error, :desynchronized},
             {:error, :busy}
           ]

    assert elem(:sys.get_state(channel), 0) == :recovery_required
  end

  test "abandoned prompts and expired leases cannot reopen admission", %{
    channel: channel,
    pty: pty
  } do
    task =
      Task.async(fn ->
        AT.session(channel, fn at ->
          :ok = AT.send(at, "AT+X\r")
          assert AT.receive(at) == {:ok, {:intermediate, "> "}}
          :unfinished
        end)
      end)

    assert PTY.read(pty, 5) == "AT+X\r"
    :ok = PTY.write(pty, "> ")
    assert Task.await(task) == :unfinished
    assert AT.session(channel, fn _ -> flunk("fenced") end) == {:error, :desynchronized}
  end

  test "idle lease expiration is safe, pending expiration is fenced", %{channel: channel} do
    for pending <- [false, true] do
      AT.session(
        channel,
        fn at ->
          if pending, do: assert(AT.send(at, "AT\r") == :ok)
          Process.sleep(40)

          assert AT.send(at, "late") ==
                   {:error, if(pending, do: :desynchronized, else: :invalid_session)}
        end,
        timeout: 20
      )

      expected = if pending, do: :recovery_required, else: :idle
      assert {^expected, %{session: nil}} = :sys.get_state(channel)
    end
  end

  test "UART process loss is fenced without killing the channel", %{channel: channel} do
    uart = elem(:sys.get_state(channel), 1).uart
    monitor = Process.monitor(uart)
    Process.exit(uart, :kill)
    assert_receive {:DOWN, ^monitor, :process, ^uart, :killed}

    assert AT.session(channel, fn at -> AT.send(at, "AT\r") end) in [
             {:error, :desynchronized},
             {:error, :io_error}
           ]

    assert Process.alive?(channel)
    assert elem(:sys.get_state(channel), 0) == :recovery_required
    assert Channel.recover(channel) == :ok
    assert AT.session(channel, fn _ -> :recovered end) == :recovered
  end

  test "UART disconnect and explicit recovery onto a replacement PTY", %{
    channel: channel,
    pty: pty
  } do
    tty = PTY.tty(pty)
    :ok = PTY.disconnect(pty)
    assert {:error, _} = AT.session(channel, &AT.command(&1, "AT", timeout: 100))
    assert AT.session(channel, fn _ -> flunk("fenced") end) == {:error, :desynchronized}
    replacement = start_supervised!({PTY, []}, id: :replacement)
    File.rm(tty)
    File.ln_s!(PTY.tty(replacement), tty)
    assert Channel.recover(channel) == :ok
    assert AT.session(channel, fn _ -> :available end) == :available
  end

  test "invalid inputs fail before I/O and duplicate ownership is rejected", %{
    channel: channel,
    pty: pty
  } do
    assert AT.session(channel, fn _ -> flunk("invalid") end, timeout: 0) ==
             {:error, :invalid_options}

    AT.session(channel, fn at ->
      assert AT.send(at, "") == {:error, :invalid_data}
      assert AT.send(at, String.duplicate("x", 4_097)) == {:error, :invalid_data}
      assert AT.receive(at, until: ">") == {:error, :invalid_options}
      assert Channel.recover(channel) == {:error, :busy}
    end)

    refute elem(:sys.get_state(channel), 0) == :recovery_required
    assert {:error, reason} = start_supervised({Channel, tty: PTY.tty(pty)}, id: :duplicate)
    assert inspect(reason) =~ "already_open"
  end

  test "an intermediate response without a preceding write still leaves an unfinished exchange",
       %{channel: channel, pty: pty} do
    task = Task.async(fn -> AT.session(channel, &AT.receive/1) end)
    :ok = PTY.write(pty, "> ")
    assert Task.await(task) == {:ok, {:intermediate, "> "}}
    assert AT.session(channel, fn _ -> flunk("fenced") end) == {:error, :desynchronized}
  end

  test "standalone channel names and tty claims follow supervised lifetimes" do
    pty = start_supervised!({PTY, []}, id: :named_pty)
    name = {:global, {__MODULE__, make_ref()}}
    start_supervised!({Channel, tty: PTY.tty(pty), name: name}, id: :named_channel)
    assert AT.session(name, fn _ -> :named end) == :named
    :ok = stop_supervised(:named_channel)
    channel = start_supervised!({Channel, tty: PTY.tty(pty)}, id: :replacement_channel)
    assert AT.session(channel, fn _ -> :reclaimed end) == :reclaimed
  end

  test "PTY supervision stops its owned OS process", %{pty: pty} do
    os_pid = :sys.get_state(pty).os_pid
    :ok = stop_supervised(PTY)
    assert {_, 1} = System.cmd("kill", ["-0", to_string(os_pid)], stderr_to_stdout: true)
  end

  test "ordinary commands ignore call-progress URCs without finishing early", %{
    channel: channel,
    pty: pty
  } do
    task = Task.async(fn -> AT.session(channel, &AT.command(&1, "AT+CGMI")) end)
    assert PTY.read(pty, 8) == "AT+CGMI\r"
    :ok = PTY.write(pty, "NO CARRIER\r\nBUSY\r\nNO ANSWER\r\n")
    assert Task.yield(task, 20) == nil
    :ok = PTY.write(pty, "Quectel\r\nOK\r\n")
    assert Task.await(task) == {:ok, ["Quectel"]}

    task = Task.async(fn -> AT.session(channel, &AT.command(&1, "ATD123")) end)
    assert PTY.read(pty, 7) == "ATD123\r"
    :ok = PTY.write(pty, "NO CARRIER\r\n")
    assert Task.await(task) == {:error, :error}
    assert AT.session(channel, fn _ -> :clean end) == :clean
  end

  test "the owner can finish a fragmented reply after a command timeout", %{
    channel: channel,
    pty: pty
  } do
    AT.session(channel, fn at ->
      task =
        Task.async(fn ->
          assert PTY.read(pty, 3) == "AT\r"
          :ok = PTY.write(pty, "NO CARRIER\r\nO")
        end)

      assert AT.command(at, "AT", timeout: 100) == {:error, :timeout}
      Task.await(task)
      refute elem(:sys.get_state(channel), 0) == :recovery_required
      assert AT.command(at, "AT") == {:error, :exchange_pending}
      :ok = PTY.write(pty, "K\r\n")
      assert AT.receive(at) == {:ok, {:response, ["NO CARRIER", "OK"]}}
    end)

    assert AT.session(channel, fn _ -> :clean end) == :clean
    assert {:ok, ""} = Circuits.UART.read(:sys.get_state(pty).peer, 0)
  end

  test "the owner can cancel a prompt with ESC after a receive timeout", %{
    channel: channel,
    pty: pty
  } do
    AT.session(
      channel,
      fn at ->
        assert AT.send(at, "AT+CMGS=1\r") == :ok
        assert PTY.read(pty, 10) == "AT+CMGS=1\r"
        :ok = PTY.write(pty, "> ")
        assert AT.receive(at) == {:ok, {:intermediate, "> "}}
        assert AT.receive(at, timeout: 20) == {:error, :timeout}
        assert AT.session(channel, fn _ -> flunk("busy") end) == {:error, :busy}
        assert AT.command(at, "AT") == {:error, :exchange_pending}
        assert AT.send(at, <<27>>) == :ok
        assert PTY.read(pty, 1) == <<27>>
        :ok = PTY.write(pty, "OK\r\n")
        assert AT.receive(at) == {:ok, {:response, ["OK"]}}
      end,
      timeout: 1_000
    )

    assert AT.session(channel, fn _ -> :clean end) == :clean
  end

  test "returning after a receive timeout or unacknowledged abort fences the channel", %{
    channel: channel,
    pty: pty
  } do
    AT.session(channel, fn at ->
      assert AT.send(at, "AT\r") == :ok
      assert PTY.read(pty, 3) == "AT\r"
      assert AT.receive(at, timeout: 20) == {:error, :timeout}
      assert AT.send(at, <<27>>) == :ok
      assert PTY.read(pty, 1) == <<27>>
      assert AT.receive(at, timeout: 20) == {:error, :timeout}
    end)

    assert AT.session(channel, fn _ -> flunk("fenced") end) == {:error, :desynchronized}
  end

  test "I/O failure still fences a session after a receive timeout", %{channel: channel, pty: pty} do
    AT.session(channel, fn at ->
      assert AT.send(at, "AT\r") == :ok
      assert PTY.read(pty, 3) == "AT\r"
      assert AT.receive(at, timeout: 20) == {:error, :timeout}
      :ok = PTY.disconnect(pty)
      assert {:error, reason} = AT.receive(at, timeout: 100)
      refute reason == :timeout
      assert AT.send(at, <<27>>) == {:error, :desynchronized}
    end)
  end

  test "queued recovery cannot execute after its caller times out", %{channel: channel} do
    assert AT.session(channel, &AT.send(&1, "AT\r")) == :ok
    uart = elem(:sys.get_state(channel), 1).uart
    :ok = :sys.suspend(channel)

    try do
      task = Task.async(fn -> Channel.recover(channel) end)
      assert Task.await(task, 7_000) == {:error, :outcome_unknown}
    after
      :sys.resume(channel)
    end

    assert {:recovery_required, %{uart: ^uart}} = :sys.get_state(channel)
  end

  test "temporary unlinked channels stop when their owner exits", %{pty: pty} do
    parent = self()
    :ok = stop_supervised(Channel)

    {owner, owner_monitor} =
      spawn_monitor(fn ->
        {:ok, channel} = Channel.start(tty: PTY.tty(pty), owner: self())
        send(parent, {:temporary_channel, channel})

        receive do
          :stop -> :ok
        end
      end)

    assert_receive {:temporary_channel, channel}
    monitor = Process.monitor(channel)
    send(owner, :stop)
    assert_receive {:DOWN, ^owner_monitor, :process, ^owner, :normal}
    assert_receive {:DOWN, ^monitor, :process, ^channel, :normal}
  end

  test "status diagnostics redact requests and payloads" do
    assert Channel.format_status(%{
             state: "private",
             data: "private",
             queue: ["private"],
             postponed: ["private"],
             timeouts: ["private"],
             message: "private",
             reason: "private",
             log: ["private"]
           }) ==
             %{
               state: :redacted,
               data: :redacted,
               queue: :redacted,
               postponed: :redacted,
               timeouts: :redacted,
               message: :redacted,
               reason: :redacted,
               log: []
             }
  end
end
