# SPDX-FileCopyrightText: 2026 Ben Youngblood
#
# SPDX-License-Identifier: Apache-2.0

defmodule VintageNetECM.ATChannelRecoveryTest do
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

  test "a pending read does not block acquisition, recovery rejection or diagnostics", %{
    channel: channel,
    pty: pty
  } do
    task =
      Task.async(fn ->
        AT.session(channel, fn at ->
          :ok = AT.send(at, "AT\r")
          AT.receive(at, timeout: 2_000)
        end)
      end)

    assert PTY.read(pty, 3) == "AT\r"
    await_reader(channel)

    started = System.monotonic_time(:millisecond)
    assert AT.session(channel, fn _ -> flunk("busy") end) == {:error, :busy}
    assert Channel.recover(channel) == {:error, :busy}
    assert System.monotonic_time(:millisecond) - started < 500
    :ok = PTY.write(pty, "OK\r\n")
    assert Task.await(task) == {:ok, {:response, ["OK"]}}
  end

  test "one session supports many prompt exchanges and completed commands", %{
    channel: channel,
    pty: pty
  } do
    AT.session(channel, fn at ->
      for _ <- 1..2 do
        :ok = AT.send(at, "AT+X\r")
        assert PTY.read(pty, 5) == "AT+X\r"

        for payload <- ["one", "two", "three"] do
          :ok = PTY.write(pty, "> ")
          assert AT.receive(at) == {:ok, {:intermediate, "> "}}
          :ok = AT.send(at, payload)
          assert PTY.read(pty, byte_size(payload)) == payload
        end

        :ok = PTY.write(pty, "OK\r\n")
        assert AT.receive(at) == {:ok, {:response, ["OK"]}}
        assert AT.session(channel, fn _ -> flunk("still owned") end) == {:error, :busy}
      end
    end)

    assert {:idle, %{session: nil}} = :sys.get_state(channel)
  end

  test "a late controller reply restores the channel without replay or a reset", %{
    channel: channel,
    pty: pty
  } do
    assert Channel.controller_session(channel, &AT.command(&1, "AT+SLOW", timeout: 20), []) ==
             {:error, :timeout}

    assert PTY.read(pty, 8) == "AT+SLOW\r"
    assert status(channel) == :waiting
    :ok = PTY.write(pty, "NO CARRIER\r\nO")
    await_buffer(channel, "\nO")
    assert status(channel) == :waiting
    :ok = PTY.write(pty, "K\r\n")
    await_state(channel, :idle)
    assert status(channel) == :ok
    assert AT.session(channel, fn _ -> :available end) == :available
    assert {:ok, ""} = Circuits.UART.read(:sys.get_state(pty).peer, 0)
  end

  test "a final modem error is not a recovery failure", %{channel: channel, pty: pty} do
    task = Task.async(fn -> Channel.controller_session(channel, &AT.command(&1, "AT"), []) end)
    assert PTY.read(pty, 3) == "AT\r"
    :ok = PTY.write(pty, "ERROR\r\n")
    assert Task.await(task) == {:error, :error}
    assert status(channel) == :ok
  end

  test "a late reply never clears an application failure", %{channel: channel, pty: pty} do
    assert AT.session(channel, &AT.command(&1, "AT", timeout: 20)) == {:error, :timeout}
    assert PTY.read(pty, 3) == "AT\r"
    :ok = PTY.write(pty, "OK\r\n")
    Process.sleep(50)
    assert status(channel) == :application_owned
    assert AT.session(channel, fn _ -> flunk("fenced") end) == {:error, :desynchronized}
  end

  test "an expired controller grace period crashes the channel", %{pty: pty} do
    :ok = stop_supervised(Channel)
    {:ok, channel} = Channel.start(tty: PTY.tty(pty), late_reply_timeout: 50)
    ref = Process.monitor(channel)

    assert Channel.controller_session(channel, &AT.command(&1, "AT", timeout: 20), []) ==
             {:error, :timeout}

    assert PTY.read(pty, 3) == "AT\r"
    assert_receive {:DOWN, ^ref, :process, _, {:controller_failure, :no_final_response}}, 500
  end

  test "controller-owned UART loss replies promptly and crashes the channel", %{
    channel: channel,
    pty: pty
  } do
    ref = Process.monitor(channel)

    task =
      Task.async(fn ->
        Channel.controller_session(channel, &AT.command(&1, "AT", timeout: 5_000), timeout: 6_000)
      end)

    assert PTY.read(pty, 3) == "AT\r"
    await_reader(channel)
    {_, data} = :sys.get_state(channel)
    Process.exit(data.uart, :kill)
    assert {:error, _} = Task.await(task, 1_000)
    assert_receive {:DOWN, ^ref, :process, _, {:controller_failure, _}}, 500
  end

  test "application-owned UART loss stays fenced", %{channel: channel} do
    AT.session(channel, fn at ->
      {_, data} = :sys.get_state(channel)
      Process.exit(data.uart, :kill)
      await_state(channel, :recovery_required)
      assert AT.send(at, "AT\r") == {:error, :desynchronized}
    end)

    Process.sleep(50)
    assert status(channel) == :application_owned
  end

  test "a completed receive timer cannot time out the next receive", %{channel: channel, pty: pty} do
    task =
      Task.async(fn ->
        AT.session(channel, fn at ->
          :ok = AT.send(at, "AT+X\r")
          assert AT.receive(at, timeout: 100) == {:ok, {:intermediate, "> "}}
          AT.receive(at, timeout: 1_000)
        end)
      end)

    assert PTY.read(pty, 5) == "AT+X\r"
    :ok = PTY.write(pty, "> ")
    Process.sleep(150)
    assert Task.yield(task, 0) == nil
    :ok = PTY.write(pty, "OK\r\n")
    assert Task.await(task) == {:ok, {:response, ["OK"]}}
  end

  test "input from an old UART is ignored after recovery", %{channel: channel} do
    {_, %{uart: old_uart}} = :sys.get_state(channel)
    assert Channel.recover(channel) == :ok
    send(channel, {:circuits_uart, old_uart, {:error, :eio}})
    send(channel, {:circuits_uart, old_uart, "OK\r\n"})
    assert {:idle, %{buffer: ""}} = :sys.get_state(channel)
  end

  test "input remains bounded even without a receive call", %{channel: channel, pty: pty} do
    AT.session(channel, fn at ->
      :ok = AT.send(at, "AT\r")
      assert PTY.read(pty, 3) == "AT\r"
      :ok = PTY.write(pty, String.duplicate("x", 16_385))
      await_failure(channel)
      assert AT.receive(at) == {:error, :desynchronized}
    end)

    assert {:recovery_required, %{reader: nil, session: nil}} = :sys.get_state(channel)
  end

  defp status(channel) do
    case :sys.get_state(channel) do
      {:recovery_required, %{failure: %{owner: :application}}} -> :application_owned
      {:recovery_required, %{failure: %{late_reply: true}}} -> :waiting
      {:recovery_required, _} -> :blocked
      _ -> :ok
    end
  end

  defp await_state(channel, state),
    do: eventually(fn -> elem(:sys.get_state(channel), 0) == state end)

  defp await_reader(channel),
    do: eventually(fn -> elem(:sys.get_state(channel, 100), 1).reader != nil end)

  defp await_buffer(channel, suffix),
    do: eventually(fn -> String.ends_with?(elem(:sys.get_state(channel), 1).buffer, suffix) end)

  defp await_failure(channel),
    do: eventually(fn -> elem(:sys.get_state(channel), 0) == :recovery_required end)

  defp eventually(fun, tries \\ 100)
  defp eventually(_, 0), do: flunk("channel did not reach expected state")

  defp eventually(fun, tries) do
    if fun.() do
      :ok
    else
      Process.sleep(5)
      eventually(fun, tries - 1)
    end
  end
end
