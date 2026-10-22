# Copyright 2026 Rahmi Pruitt
# SPDX-License-Identifier: Apache-2.0

defmodule Spillway.WebSocketSessionValkeyTest do
  # Runs against a real Valkey that allows DEBUG, e.g.
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

    {:ok, session} =
      WebSocketSession.start_link(
        session_id: session_id,
        upstream_url: "ws://127.0.0.1:1/ws",
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

    assert_receive {:websocket_upstream_frame_recorded, ^ref}, 20_000
    assert Process.alive?(session)
    Task.await(stall, 10_000)

    assert {:ok, [%{envelope: %{"message_id" => "e1"}}]} =
             WebSocketStore.read_after(session_id, "0-0", 10)
  end
end
