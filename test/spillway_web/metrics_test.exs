# Copyright 2026 Rahmi Pruitt
# SPDX-License-Identifier: Apache-2.0

defmodule SpillwayWeb.MetricsTest do
  # Reads the node-wide metrics table and switches the adapter.
  use ExUnit.Case, async: false

  import Phoenix.ConnTest

  alias SpillwayWeb.{JobController, MetricsController}

  defmodule Backend do
    import Plug.Conn

    def init(test_pid), do: test_pid

    def call(conn, test_pid) do
      send(test_pid, {:seen, conn.request_path, get_req_header(conn, "x-aqueduct-node-id")})
      send_resp(conn, 200, "ok")
    end
  end

  defp start_backend do
    port = Enum.random(20_000..60_000)

    start_supervised!(
      Supervisor.child_spec({Bandit, plug: {Backend, self()}, port: port}, id: :"metrics_#{port}")
    )

    "http://127.0.0.1:#{port}"
  end

  defp dispatches(acc) when length(acc) == 6, do: acc

  defp dispatches(acc) do
    receive do
      {:seen, path, ids} when path in ["/work", "/hook"] -> dispatches([{path, ids} | acc])
      {:seen, _path, _ids} -> dispatches(acc)
    after
      1_000 -> acc
    end
  end

  defp scrape do
    conn = MetricsController.index(build_conn(:get, "/metrics"), %{})
    assert conn.status == 200
    conn.resp_body
  end

  defp value(body, series) do
    case Regex.run(~r/^#{Regex.escape(series)} (\S+)$/m, body) do
      [_, v] -> String.to_integer(v)
      nil -> 0
    end
  end

  # Through a real backend: jobs and their completion webhooks show up on
  # /metrics, and the backend sees the node's id.
  test "dispatched jobs show up on /metrics and carry X-Aqueduct-Node-Id" do
    url = start_backend()
    target = Spillway.Metrics.upstream(url)
    before = scrape()
    jobs_series = ~s(spillway_jobs_total{target="#{target}",outcome="completed"})
    webhooks_series = ~s(spillway_webhooks_total{outcome="delivered"})

    for i <- 1..3 do
      params = %{
        "user_id" => "metrics-#{i}",
        "idempotent_key" => "metrics-#{System.unique_integer([:positive])}",
        "url" => url <> "/work",
        "method" => "POST",
        "webhook_url" => url <> "/hook"
      }

      conn = JobController.create(build_conn(:post, "/jobs", params), params)
      assert conn.status == 201
    end

    body =
      Enum.reduce_while(1..100, before, fn _, _ ->
        body = scrape()

        if value(body, webhooks_series) - value(before, webhooks_series) >= 3,
          do: {:halt, body},
          else: Process.sleep(50) && {:cont, body}
      end)

    assert value(body, jobs_series) - value(before, jobs_series) == 3

    attempts = ~s(spillway_dispatch_attempts_total{target="#{target}",status_class="2xx"})
    assert value(body, attempts) - value(before, attempts) == 3
    assert value(body, webhooks_series) - value(before, webhooks_series) == 3
    assert body =~ ~s(spillway_queue_wait_seconds_count{target="#{target}"})
    assert body =~ ~s(spillway_job_duration_seconds_count{target="#{target}",outcome="completed"})
    assert body =~ ~s(spillway_node_info{node_id="#{Spillway.NodeId.get()}"} 1)
    assert body =~ "# TYPE spillway_active_queues gauge"

    # Every job and webhook carried the node's id. (The L8 discovery fetch
    # of /.well-known/l8 isn't a dispatch.)
    seen = dispatches([])
    assert Enum.count(seen, &(elem(&1, 0) == "/work")) == 3
    assert Enum.count(seen, &(elem(&1, 0) == "/hook")) == 3
    assert Enum.all?(seen, &(elem(&1, 1) == [Spillway.NodeId.get()]))
  end

  test "a refused job is counted by reason" do
    Spillway.Metrics.admission_rejected("memory")
    assert scrape() =~ ~r/^spillway_admission_rejected_total\{reason="memory"\} [1-9]/m
  end

  test "/metrics is 404 with another adapter" do
    Application.put_env(:spillway, :metrics_adapter, Spillway.Metrics.Noop)

    on_exit(fn ->
      Application.put_env(:spillway, :metrics_adapter, Spillway.Metrics.Prometheus)
    end)

    assert MetricsController.index(build_conn(:get, "/metrics"), %{}).status == 404
  end

  test "the node id is the first of SPILLWAY_NODE_ID, FLY_MACHINE_ID and the host name" do
    env = fn vars -> &Map.get(vars, &1) end
    host = fn -> {:ok, ~c"box"} end

    assert Spillway.NodeId.resolve(
             env.(%{"SPILLWAY_NODE_ID" => " a ", "FLY_MACHINE_ID" => "b"}),
             host
           ) == "a"

    assert Spillway.NodeId.resolve(
             env.(%{"SPILLWAY_NODE_ID" => " ", "FLY_MACHINE_ID" => "b"}),
             host
           ) == "b"

    assert Spillway.NodeId.resolve(env.(%{}), host) == "box"
    assert Spillway.NodeId.resolve(env.(%{}), fn -> {:error, :nxdomain} end) == "unknown"
  end
end
