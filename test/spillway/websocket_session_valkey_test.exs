# Copyright 2026 Rahmi Pruitt
# SPDX-License-Identifier: Apache-2.0

defmodule Spillway.WebSocketSessionValkeyTest do
  # Runs against a dedicated Valkey that allows DEBUG; setup runs FLUSHDB,
  # as websocket_export_valkey_test.exs does, so don't point it at a shared
  # instance. For example:
  #   docker run -d -p 16380:6379 valkey/valkey:8 valkey-server --enable-debug-command yes
  #   SPILLWAY_TEST_VALKEY_URL=redis://localhost:16380 mix test
  use ExUnit.Case, async: false

  alias Spillway.{WebSocketConfig, WebSocketSession, WebSocketStore}
  alias Spillway.WebSocketStore.Valkey

  @url System.get_env("SPILLWAY_TEST_VALKEY_URL")
  @moduletag skip:
               if(@url,
                 do: false,
                 else: "set SPILLWAY_TEST_VALKEY_URL to run against a real Valkey"
               )

  defmodule QuietBackend do
    @behaviour WebSock
    @impl true
    def init(state), do: {:ok, state}
    @impl true
    def handle_in(_frame, state), do: {:ok, state}
    @impl true
    def handle_info(_message, state), do: {:ok, state}
  end

  defmodule BackendPlug do
    def init(opts), do: opts

    def call(conn, _opts) do
      conn
      |> Plug.Conn.put_resp_header("sec-websocket-protocol", "aqueduct.v1")
      |> WebSockAdapter.upgrade(QuietBackend, %{}, timeout: :infinity)
      |> Plug.Conn.halt()
    end
  end

  setup do
    System.put_env("SPILLWAY_VALKEY_URL", @url)
    {:ok, admin} = Redix.start_link(@url)
    Redix.command!(admin, ["FLUSHDB"])
    for spec <- Valkey.child_specs(), do: start_supervised!(spec)
    on_exit(fn -> System.delete_env("SPILLWAY_VALKEY_URL") end)
    %{admin: admin}
  end

  # The soak saw sessions crash on "event store unavailable: Redix timeout"
  # right after load bursts, losing the backend message being recorded.
  test "a backend message is still recorded after the event store stalls past the Redix timeout",
       %{admin: admin} do
    session_id = "stall-#{System.unique_integer([:positive])}"
    # A reachable backend, so a reconnect that isn't held back would
    # actually replace the upstream.
    port = Enum.random(20_000..60_000)
    start_supervised!({Bandit, plug: BackendPlug, port: port})

    {:ok, session} =
      WebSocketSession.start_link(
        session_id: session_id,
        upstream_url: "ws://127.0.0.1:#{port}/ws",
        config: WebSocketConfig.load()
      )

    # Stand in for the upstream connection, so the frame below is accepted
    # and its acknowledgement comes back here.
    test_pid = self()
    :sys.replace_state(session, &%{&1 | upstream_pid: test_pid})
    ref = make_ref()

    # Freeze Valkey for 6 s, longer than Redix's 5 s timeout.
    stall = Task.async(fn -> Redix.command(admin, ["DEBUG", "SLEEP", "6"], timeout: 10_000) end)
    Process.sleep(100)

    frame = Jason.encode!(%{"type" => "event", "message_id" => "e1", "payload" => %{"n" => 1}})
    send(session, {:websocket_upstream_frame, self(), ref, frame})

    # Once the first write has timed out and a retry is pending, a
    # reconnect must wait: a new upstream's frames would otherwise be
    # recorded ahead of this one.
    wait_until(fn -> :sys.get_state(session, 15_000).pending_record == ref end, 10_000)
    send(session, :connect_upstream)
    Process.sleep(300)
    assert :sys.get_state(session, 15_000).upstream_pid == test_pid

    assert_receive {:websocket_upstream_frame_recorded, ^ref}, 20_000
    assert Process.alive?(session)
    assert :sys.get_state(session, 15_000).pending_record == nil
    Task.await(stall, 10_000)

    assert {:ok, [%{envelope: %{"message_id" => "e1"}}]} =
             WebSocketStore.read_after(session_id, "0-0", 10)
  end

  # The timed-out write lands, then a client command is recorded before the
  # retry runs: the transcript's newest entry is the command, and the retry
  # must still find the frame instead of writing it twice.
  test "a retry finds its frame even when a client command was recorded after it",
       %{admin: admin} do
    session_id = "stall-cmd-#{System.unique_integer([:positive])}"

    {:ok, session} =
      WebSocketSession.start_link(
        session_id: session_id,
        upstream_url: "ws://127.0.0.1:1/ws",
        config: WebSocketConfig.load()
      )

    test_pid = self()
    :sys.replace_state(session, &%{&1 | upstream_pid: test_pid})
    ref = make_ref()

    stall = Task.async(fn -> Redix.command(admin, ["DEBUG", "SLEEP", "6"], timeout: 10_000) end)
    Process.sleep(100)

    frame = Jason.encode!(%{"type" => "event", "message_id" => "e1", "payload" => %{"n" => 1}})
    send(session, {:websocket_upstream_frame, self(), ref, frame})
    Process.sleep(50)

    # Queued behind the frame on the same connection, so it lands right after.
    command =
      Task.async(fn ->
        WebSocketStore.append(session_id, "client", %{"type" => "command", "message_id" => "c1"})
      end)

    assert_receive {:websocket_upstream_frame_recorded, ^ref}, 20_000
    Task.await(stall, 10_000)
    Task.await(command, 10_000)

    assert {:ok, entries} = WebSocketStore.read_after(session_id, "0-0", 10)
    assert Enum.map(entries, & &1.envelope["message_id"]) == ["e1", "c1"]
  end

  defp wait_until(fun, timeout_ms) do
    cond do
      fun.() -> :ok
      timeout_ms <= 0 -> flunk("condition not met")
      true -> Process.sleep(20) && wait_until(fun, timeout_ms - 20)
    end
  end
end
