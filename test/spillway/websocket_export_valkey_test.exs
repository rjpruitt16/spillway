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
