# Copyright 2026 Rahmi Pruitt
# SPDX-License-Identifier: Apache-2.0

defmodule Spillway.RunnerMonitorTest do
  @moduledoc """
  Each dispatched job runs in its own process. Those used to be unmonitored,
  so one that died mid-job kept its slot counted in flight and left its job
  unfinished in the store and out of the queue until the node restarted
  (soak run 3l: about 2,000 stuck jobs after a load peak, recovered only by
  the next kill).
  """

  use ExUnit.Case, async: false

  alias Spillway.{AccountQueueRegistry, Cluster, IdempotentStore, Job}

  # The first request hangs, so the test can kill the runner while it waits;
  # later ones answer 200. Every request is reported as {:attempt, path}.
  defmodule HangOncePlug do
    import Plug.Conn
    def init(opts), do: opts

    def call(%{request_path: "/.well-known/l8"} = conn, _opts), do: send_resp(conn, 404, "")

    def call(conn, %{counter: counter, test_pid: test_pid}) do
      send(test_pid, {:attempt, conn.request_path})

      if :counters.get(counter, 1) == 0 do
        :counters.add(counter, 1, 1)
        Process.sleep(3_000)
      end

      send_resp(conn, 200, ~s({"ok":true}))
    end
  end

  defmodule WebhookPlug do
    import Plug.Conn
    def init(opts), do: opts

    def call(conn, test_pid) do
      {:ok, body, conn} = read_body(conn)

      case Jason.decode(body) do
        {:ok, %{"job_id" => _} = payload} -> send(test_pid, {:webhook, payload})
        _ -> :ok
      end

      send_resp(conn, 200, "ok")
    end
  end

  setup do
    previous =
      for key <- [:default_rps, :dispatch_retry_ms],
          do: {key, Application.get_env(:spillway, key)}

    Application.put_env(:spillway, :default_rps, 100.0)
    Application.put_env(:spillway, :dispatch_retry_ms, 0)

    on_exit(fn ->
      for {key, value} <- previous do
        if value == nil,
          do: Application.delete_env(:spillway, key),
          else: Application.put_env(:spillway, key, value)
      end
    end)

    {:ok, webhook: start_server(WebhookPlug, self())}
  end

  defp start_server(plug, opts) do
    port = Enum.random(20_000..60_000)

    start_supervised!(
      Supervisor.child_spec({Bandit, plug: {plug, opts}, port: port}, id: :"runner_#{port}")
    )

    "http://127.0.0.1:#{port}"
  end

  defp hang_once_upstream do
    start_server(HangOncePlug, %{counter: :counters.new(1, []), test_pid: self()})
  end

  defp submit(url, webhook, extra \\ %{}) do
    {:ok, job} =
      %{
        "user_id" => "runner-user",
        "idempotent_key" => "runner-#{System.unique_integer([:positive])}",
        "url" => url,
        "method" => "POST",
        "webhook_url" => webhook
      }
      |> Map.merge(extra)
      |> Job.new()

    :ok = IdempotentStore.check_or_insert(job)
    AccountQueueRegistry.enqueue(job, "")
    job
  end

  defp queue_for(base, job) do
    wait_for(fn ->
      Cluster.lookup_account_queue(base, Job.queue_key(job)) ||
        Cluster.lookup_account_queue(base, :shared)
    end)
  end

  defp kill_runner(queue) do
    [pid] = wait_for(fn -> Map.keys(:sys.get_state(queue).runners) |> then(&(&1 != [] && &1)) end)
    Process.exit(pid, :kill)
  end

  defp settled?(queue) do
    state = :sys.get_state(queue)
    state.in_flight == 0 and state.runners == %{}
  end

  test "a job whose runner dies is retried and completes", %{webhook: webhook} do
    base = hang_once_upstream()
    job = submit(base <> "/x", webhook)
    assert_receive {:attempt, "/x"}, 5_000

    queue = queue_for(base, job)
    kill_runner(queue)

    assert_receive {:attempt, "/x"}, 5_000
    assert_receive {:webhook, %{"status" => "completed"}}, 5_000
    assert IdempotentStore.get_status(job.id) == "completed"
    assert IdempotentStore.get_job(job.id).attempts == 1
    assert wait_for(fn -> settled?(queue) end)
  end

  test "a job with no retries left fails when its runner dies", %{webhook: webhook} do
    base = hang_once_upstream()
    job = submit(base <> "/x", webhook, %{"max_retries" => 0})
    assert_receive {:attempt, "/x"}, 5_000

    queue = queue_for(base, job)
    kill_runner(queue)

    assert_receive {:webhook, %{"status" => "failed", "reason" => reason}}, 5_000
    assert reason =~ "job_runner_exited"
    assert wait_for(fn -> IdempotentStore.get_status(job.id) == "failed" end)
    assert wait_for(fn -> settled?(queue) end)
    refute_received {:attempt, "/x"}
  end

  test "finished runners are not kept around", %{webhook: webhook} do
    base = hang_once_upstream()
    # Use up the hanging request on a job nobody waits for.
    first = submit(base <> "/warm", webhook, %{"max_retries" => 0})
    assert_receive {:attempt, "/warm"}, 5_000
    queue = queue_for(base, first)
    kill_runner(queue)
    assert_receive {:webhook, %{"job_id" => first_id}}, 5_000
    assert first_id == first.id

    for _ <- 1..20, do: submit(base <> "/y", webhook)
    for _ <- 1..20, do: assert_receive({:webhook, %{"status" => "completed"}}, 5_000)
    assert wait_for(fn -> settled?(queue) end)
  end

  defp wait_for(fun, timeout_ms \\ 3_000) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms

    Stream.repeatedly(fn ->
      value = fun.()
      if value in [nil, false], do: Process.sleep(20)
      value
    end)
    |> Enum.find(fn value ->
      value not in [nil, false] or System.monotonic_time(:millisecond) > deadline
    end)
  end
end
