# Copyright 2026 Rahmi Pruitt
# SPDX-License-Identifier: Apache-2.0

defmodule Spillway.WebSocketExportTest do
  # The export queue and the transcript store are shared application state.
  use ExUnit.Case, async: false

  alias Spillway.{Store, WebSocketExport, WebSocketStore}
  alias Spillway.WebSocketExport.{Exporter, LocalQueue}

  # Records batches. respond.(batch) picks the status code.
  defmodule Receiver do
    import Plug.Conn
    def init(opts), do: opts

    def call(conn, %{test: test, respond: respond}) do
      {:ok, body, conn} = read_body(conn, length: 50_000_000)
      batch = Jason.decode!(body)
      status = respond.(batch)
      send(test, {:batch, status, batch, get_req_header(conn, "authorization")})
      send(test, {:auth, get_req_header(conn, "authorization"), batch["schema"]})
      send_resp(conn, status, "")
    end
  end

  setup do
    Store.clear([:ws_export])

    on_exit(fn ->
      WebSocketExport.set_enabled(false)
      Store.clear([:ws_export])
    end)

    :ok
  end

  defp receiver(respond \\ fn _ -> 200 end) do
    port = Enum.random(20_000..60_000)
    start_supervised!({Bandit, plug: {Receiver, %{test: self(), respond: respond}}, port: port})
    "http://127.0.0.1:#{port}/batches"
  end

  defp start_exporter(url, overrides \\ %{}) do
    config = struct(%WebSocketExport{enabled: true, url: url, token: "tok"}, overrides)
    start_supervised!({Exporter, config})
  end

  defp append(session, type, id, payload \\ %{"n" => 1}) do
    {:ok, entry} =
      WebSocketStore.append(session, "client", %{
        "type" => type,
        "message_id" => id,
        "payload" => payload
      })

    entry.stream_id
  end

  defp collect(want, acc \\ []) do
    if length(acc) >= want do
      acc
    else
      receive do
        {:batch, status, batch, _auth} when status in 200..299 ->
          collect(want, acc ++ batch["events"])

        {:batch, _status, _batch, _auth} ->
          collect(want, acc)
      after
        10_000 -> flunk("got #{length(acc)} of #{want} events")
      end
    end
  end

  defp wait_until(fun, tries \\ 200) do
    cond do
      fun.() -> :ok
      tries == 0 -> flunk("condition not met")
      true -> Process.sleep(25) && wait_until(fun, tries - 1)
    end
  end

  test "the URL must be https, except loopback or when insecure is allowed" do
    assert :ok = WebSocketExport.check_url("https://inlet.example/_inlet/websocket-events", false)
    assert :ok = WebSocketExport.check_url("http://localhost:9000/b", false)
    assert :ok = WebSocketExport.check_url("http://127.0.0.1:9000/b", false)
    assert :ok = WebSocketExport.check_url("http://[::1]:9000/b", false)
    assert {:error, _} = WebSocketExport.check_url("http://inlet.example/b", false)
    assert {:error, _} = WebSocketExport.check_url("/relative", false)
    assert :ok = WebSocketExport.check_url("http://inlet.internal/b", true)
  end

  test "export is off by default and without a URL" do
    System.delete_env("SPILLWAY_WS_EXPORT_ENABLED")
    refute WebSocketExport.load_config().enabled

    System.put_env("SPILLWAY_WS_EXPORT_ENABLED", "true")
    System.delete_env("SPILLWAY_WS_EXPORT_URL")
    on_exit(fn -> System.delete_env("SPILLWAY_WS_EXPORT_ENABLED") end)
    refute WebSocketExport.load_config().enabled
  end

  test "every appended message is exported once with its stream_id, then deleted from the queue" do
    url = receiver()
    start_exporter(url)

    ids = %{
      "m1" => append("export-a", "command", "m1"),
      "m2" => append("export-b", "command", "m2"),
      "m3" => append("export-a", "command", "m3")
    }

    events = collect(3)
    assert Enum.sort(Enum.map(events, & &1["message_id"])) == ["m1", "m2", "m3"]

    for event <- events do
      assert event["stream_id"] == ids[event["message_id"]]
      assert event["direction"] == "client"
      assert event["payload"] == %{"n" => 1}
      assert event["session_id"] in ["export-a", "export-b"]
    end

    assert_received {:auth, ["Bearer tok"], "aquifer.websocket_batch.v1"}
    wait_until(fn -> LocalQueue.read(10) == [] end)
    assert {:ok, transcript} = WebSocketStore.read_after("export-a", "0-0", 10)
    assert length(transcript) == 2, "the transcript keeps its entries for replay"
  end

  test "a failed batch is retried until the receiver accepts it" do
    attempts = :counters.new(1, [])

    url =
      receiver(fn _ ->
        :counters.add(attempts, 1, 1)
        if :counters.get(attempts, 1) <= 2, do: 503, else: 200
      end)

    start_exporter(url)
    append("export-r", "command", "r1")
    assert [%{"message_id" => "r1"}] = collect(1)
    assert :counters.get(attempts, 1) >= 3
  end

  test "a 413 halves the batch, and one message that is still too large is skipped" do
    url =
      receiver(fn %{"events" => events} ->
        cond do
          Enum.any?(events, &(&1["message_id"] == "huge")) -> 413
          length(events) > 2 -> 413
          true -> 200
        end
      end)

    # Queue everything before the exporter starts, so its first batch holds all six.
    WebSocketExport.LocalQueue.init!()
    WebSocketExport.set_enabled(%{max_pending: 1000})
    for id <- ~w(a b huge c d e), do: append("export-big", "command", id)
    start_exporter(url, %{batch: 6})

    delivered = collect(5)
    assert Enum.sort(Enum.map(delivered, & &1["message_id"])) == ~w(a b c d e)
    wait_until(fn -> LocalQueue.read(10) == [] end)
  end

  test "batches are capped by encoded size" do
    url = receiver()
    WebSocketExport.LocalQueue.init!()
    WebSocketExport.set_enabled(%{max_pending: 1000})
    # About 500 encoded bytes each: two fit under 1,200, three don't.
    for i <- 1..4,
        do: append("export-bytes", "command", "b#{i}", %{"blob" => String.duplicate("x", 400)})

    start_exporter(url, %{batch: 10, max_batch_bytes: 1_200})

    sizes = collect_sizes(4, [])

    assert Enum.max(sizes) == 2,
           "expected batches of at most two, and at least one pair: #{inspect(sizes)}"
  end

  defp collect_sizes(want, sizes) do
    if Enum.sum(sizes) >= want do
      sizes
    else
      receive do
        {:batch, 200, batch, _} -> collect_sizes(want, sizes ++ [length(batch["events"])])
      after
        10_000 -> flunk("only #{Enum.sum(sizes)} of #{want} exported")
      end
    end
  end

  test "payloads can be left out" do
    url = receiver()
    start_exporter(url, %{payloads: false})
    append("export-p", "command", "p1", %{"secret" => true})
    assert [event] = collect(1)
    refute Map.has_key?(event, "payload")
  end

  test "without export, appends queue nothing" do
    WebSocketExport.set_enabled(false)
    append("export-off", "command", "o1")
    assert Store.take_from(:ws_export, <<0::64>>, 10, fn _ -> true end) == []
  end
end
