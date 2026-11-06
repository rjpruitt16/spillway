# Copyright 2026 Rahmi Pruitt
# SPDX-License-Identifier: Apache-2.0

defmodule Spillway.QueuedTTLTest do
  # A job that waits in the queue past the queued-job TTL used to be deleted
  # by store cleanup with no webhook (#5). The queue now fails it first.
  use ExUnit.Case, async: false

  alias Spillway.{AccountQueue, IdempotentStore, Job}

  defmodule Receiver do
    @moduledoc false
    import Plug.Conn

    def init(opts), do: opts

    def call(%Plug.Conn{request_path: "/.well-known/l8"} = conn, _opts),
      do: send_resp(conn, 404, "")

    # The upstream: holds every request, so the queue's only slot stays busy.
    def call(%Plug.Conn{request_path: "/hang"} = conn, _opts) do
      Process.sleep(10_000)
      send_resp(conn, 200, "")
    end

    def call(conn, _opts) do
      {:ok, body, conn} = read_body(conn)
      if pid = Process.whereis(:queued_ttl_test), do: send(pid, {:webhook, Jason.decode!(body)})
      send_resp(conn, 200, "")
    end
  end

  setup do
    Process.register(self(), :queued_ttl_test)
    port = 23_000 + rem(System.unique_integer([:positive]), 5_000)
    {:ok, server} = Bandit.start_link(plug: Receiver, port: port, startup_log: false)
    on_exit(fn -> if Process.alive?(server), do: Process.exit(server, :kill) end)
    {:ok, base: "http://localhost:#{port}"}
  end

  test "deadline is execute_before when set, otherwise the queued TTL after submission" do
    now = System.system_time(:millisecond)
    assert Job.queue_deadline_ms(%Job{created_at: now, execute_before: now + 5}) == now + 5
    assert Job.queue_deadline_ms(%Job{created_at: now}) == now + Job.queued_ttl_ms()
    assert Job.queue_deadline_ms(%Job{created_at: nil}) == nil
  end

  test "a job queued past the TTL fails with a webhook instead of vanishing", %{base: base} do
    job = job(base, "/unused", System.system_time(:millisecond) - Job.queued_ttl_ms() - 1_000)
    queue = start_queue(base)

    assert {:accepted, _} = AccountQueue.submit(queue, job)
    assert_receive {:webhook, %{"job_id" => id, "status" => "failed", "reason" => reason}}, 5_000
    assert id == job.id
    assert reason == "queued_ttl_exceeded"
    assert IdempotentStore.get_result(job.id)["reason"] == "queued_ttl_exceeded"
  end

  test "a caller's own execute_before keeps its own reason", %{base: base} do
    now = System.system_time(:millisecond)
    job = %{job(base, "/unused", now) | execute_before: now - 1}
    queue = start_queue(base)

    assert {:accepted, _} = AccountQueue.submit(queue, job)
    assert_receive {:webhook, %{"reason" => "execution_deadline_exceeded"}}, 5_000
  end

  test "a job still expires while every slot is stuck on a hung upstream", %{base: base} do
    previous = Application.get_env(:spillway, :idempotent_ttl)
    Application.put_env(:spillway, :idempotent_ttl, 1)

    on_exit(fn ->
      if previous,
        do: Application.put_env(:spillway, :idempotent_ttl, previous),
        else: Application.delete_env(:spillway, :idempotent_ttl)
    end)

    queue = start_queue(base)
    now = System.system_time(:millisecond)
    busy = job(base, "/hang", now)
    waiting = job(base, "/hang", now)

    assert {:accepted, _} = AccountQueue.submit(queue, busy)
    assert {:accepted, _} = AccountQueue.submit(queue, waiting)

    # No completion arrives to wake the dispatch loop; the 2 s position tick
    # has to find the deadline.
    assert_receive {:webhook, %{"job_id" => id, "reason" => "queued_ttl_exceeded"}}, 6_000
    assert id == waiting.id
  end

  defp start_queue(base) do
    {:ok, queue} =
      AccountQueue.start_link(
        queue_key: "queued-ttl-#{System.unique_integer([:positive])}",
        upstream: base,
        max_concurrent: 1,
        rps: 100.0
      )

    on_exit(fn -> if Process.alive?(queue), do: Process.exit(queue, :kill) end)
    queue
  end

  defp job(base, path, created_at) do
    stamp = System.unique_integer([:positive])

    %Job{
      id: "queued-ttl-#{stamp}",
      user_id: "queued-ttl-user",
      idempotent_key: "queued-ttl-key-#{stamp}",
      url: base <> path,
      method: "POST",
      headers: %{},
      body: "",
      webhook_url: base <> "/hook",
      kind: :user,
      status: :queued,
      created_at: created_at
    }
  end
end
