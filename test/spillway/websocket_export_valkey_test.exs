# Copyright 2026 Rahmi Pruitt
# SPDX-License-Identifier: Apache-2.0

defmodule Spillway.WebSocketExportValkeyTest do
  # Runs against a real Valkey when SPILLWAY_TEST_VALKEY_URL is set, e.g.
  #   docker run -d -p 16380:6379 valkey/valkey:8
  #   SPILLWAY_TEST_VALKEY_URL=redis://localhost:16380 mix test
  use ExUnit.Case, async: false

  alias Spillway.{WebSocketExport, WebSocketStore}
  alias Spillway.WebSocketExport.{Exporter, ValkeyQueue}
  alias Spillway.WebSocketStore.Valkey

  @url System.get_env("SPILLWAY_TEST_VALKEY_URL")
  @moduletag skip:
               if(@url,
                 do: false,
                 else: "set SPILLWAY_TEST_VALKEY_URL to run against a real Valkey"
               )

  defmodule Receiver do
    import Plug.Conn
    def init(opts), do: opts

    def call(conn, test) do
      {:ok, body, conn} = read_body(conn)
      send(test, {:batch, Jason.decode!(body)})
      send_resp(conn, 200, "")
    end
  end

  setup do
    System.put_env("SPILLWAY_VALKEY_URL", @url)
    {:ok, admin} = Redix.start_link(@url)
    Redix.command!(admin, ["FLUSHDB"])
    for spec <- Valkey.child_specs(), do: start_supervised!(spec)

    on_exit(fn ->
      WebSocketExport.set_enabled(false)
      System.delete_env("SPILLWAY_VALKEY_URL")
    end)

    %{admin: admin}
  end

  test "messages are exported with the transcript's stream_id and deleted after the 2xx", %{
    admin: admin
  } do
    port = Enum.random(20_000..60_000)
    start_supervised!({Bandit, plug: {Receiver, self()}, port: port})

    exporter = %WebSocketExport{
      enabled: true,
      url: "http://127.0.0.1:#{port}/b",
      max_pending: 1000
    }

    start_supervised!({Exporter, exporter})

    ids =
      for {session, id} <- [{"vk-a", "m1"}, {"vk-b", "m2"}, {"vk-a", "m3"}], into: %{} do
        {:ok, entry} =
          WebSocketStore.append(session, "client", %{
            "type" => "command",
            "message_id" => id,
            "payload" => %{}
          })

        {id, entry.stream_id}
      end

    events = receive_events(3, [])
    for event <- events, do: assert(event["stream_id"] == ids[event["message_id"]])

    assert eventually(fn -> Redix.command!(admin, ["XLEN", ValkeyQueue.key()]) == 0 end)

    assert {:ok, [_, _]} = WebSocketStore.read_after("vk-a", "0-0", 10),
           "transcripts keep their entries"

    stop_supervised!(Exporter)

    assert Redix.command!(admin, ["XINFO", "CONSUMERS", ValkeyQueue.key(), "spillway-export"]) ==
             [],
           "a stopped exporter with nothing pending leaves the group"
  end

  test "a pending entry trimmed from the stream is acknowledged, not left pending", %{
    admin: admin
  } do
    key = ValkeyQueue.key()
    Redix.command!(admin, ["XGROUP", "CREATE", key, "spillway-export", "0", "MKSTREAM"])
    Redix.command!(admin, ["XADD", key, "*", "session_id", "s", "stream_id", "1-1"])
    # A consumer that went away read it (so it's pending), then MAXLEN trimmed it.
    Redix.command!(admin, [
      "XREADGROUP",
      "GROUP",
      "spillway-export",
      "gone-node",
      "STREAMS",
      key,
      ">"
    ])

    Redix.command!(admin, ["XTRIM", key, "MAXLEN", "0"])

    port = Enum.random(20_000..60_000)
    start_supervised!({Bandit, plug: {Receiver, self()}, port: port})
    # Claim immediately instead of waiting the usual minute of idleness.
    Application.put_env(:spillway, :ws_export_claim_idle_ms, 0)
    on_exit(fn -> Application.delete_env(:spillway, :ws_export_claim_idle_ms) end)

    start_supervised!(
      {Exporter,
       %WebSocketExport{enabled: true, url: "http://127.0.0.1:#{port}/b", max_pending: 1000}}
    )

    assert eventually(
             fn ->
               {:ok, conn} = Redix.start_link(@url)
               [count | _] = Redix.command!(conn, ["XPENDING", key, "spillway-export"])
               Redix.stop(conn)
               count == 0
             end,
             4_000
           ),
           "the trimmed entry should leave the pending list"
  end

  test "a stale entry deep in the pending list is still claimed", %{admin: admin} do
    key = ValkeyQueue.key()
    {:ok, conn} = Redix.start_link(@url)
    :ok = ValkeyQueue.ensure_group(conn)
    Redix.stop(conn)

    add = fn id ->
      Redix.command!(admin, [
        "XADD",
        key,
        "*",
        "session_id",
        "s",
        "stream_id",
        "1-#{id}",
        "direction",
        "client",
        "envelope",
        Jason.encode!(%{"type" => "command", "message_id" => id}),
        "recorded_at",
        "1"
      ])
    end

    read_as = fn consumer, count ->
      Redix.command!(admin, [
        "XREADGROUP",
        "GROUP",
        "spillway-export",
        consumer,
        "COUNT",
        "#{count}",
        "STREAMS",
        key,
        ">"
      ])
    end

    # 50 entries a live node is still working on (fresh, so not claimable),
    # then one a departed node left two minutes ago. With a batch of 1,
    # XAUTOCLAIM scans about 10 pending entries per call, so the stale entry
    # is only reached if each scan resumes where the last one stopped.
    for i <- 1..50, do: add.("busy-#{i}")
    read_as.("busy-node", 50)
    stale = add.("stale")
    read_as.("gone-node", 1)

    Redix.command!(admin, [
      "XCLAIM",
      key,
      "spillway-export",
      "gone-node",
      "0",
      stale,
      "IDLE",
      "120000"
    ])

    port = Enum.random(20_000..60_000)
    start_supervised!({Bandit, plug: {Receiver, self()}, port: port})

    start_supervised!(
      {Exporter,
       %WebSocketExport{
         enabled: true,
         url: "http://127.0.0.1:#{port}/b",
         max_pending: 1000,
         batch: 1
       }}
    )

    assert_receive {:batch, %{"events" => [%{"message_id" => "stale"}]}}, 20_000
  end

  defp receive_events(want, acc) do
    if length(acc) >= want do
      acc
    else
      receive do
        {:batch, batch} -> receive_events(want, acc ++ batch["events"])
      after
        10_000 -> flunk("got #{length(acc)} of #{want}")
      end
    end
  end

  defp eventually(fun, tries \\ 200) do
    cond do
      fun.() -> true
      tries == 0 -> false
      true -> Process.sleep(25) && eventually(fun, tries - 1)
    end
  end
end
