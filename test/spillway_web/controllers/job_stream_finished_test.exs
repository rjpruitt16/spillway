# Copyright 2026 Rahmi Pruitt
# SPDX-License-Identifier: Apache-2.0

defmodule SpillwayWeb.JobStreamFinishedTest do
  # GET /jobs/:id/stream for a job that finished before the client subscribed
  # used to send queued and dispatching, then only keepalives (#91).
  use ExUnit.Case, async: false

  import Phoenix.ConnTest

  alias Spillway.{IdempotentStore, Job}
  alias SpillwayWeb.JobStreamController

  defp finished_job(status, payload) do
    stamp = System.unique_integer([:positive])

    job = %Job{
      id: "stream-done-#{stamp}",
      user_id: "stream-user",
      idempotent_key: "stream-done-key-#{stamp}",
      url: "http://127.0.0.1:1/unused",
      method: "POST",
      headers: %{},
      body: "",
      webhook_url: "",
      kind: :user,
      status: :queued,
      created_at: System.system_time(:millisecond)
    }

    :ok = IdempotentStore.check_or_insert(job)
    IdempotentStore.put_result(job.id, Map.put(payload, :job_id, job.id), status)
    IdempotentStore.finish(job.id, status, nil)
    job
  end

  # The stream must end on its own; a hang shows up as a timeout here.
  defp stream_body(job) do
    task = Task.async(fn -> JobStreamController.stream(build_conn(), %{"id" => job.id}) end)

    case Task.yield(task, 3_000) || Task.shutdown(task) do
      {:ok, conn} -> conn.resp_body
      nil -> flunk("stream for a finished job didn't end")
    end
  end

  test "a completed job's stream sends its result and ends" do
    job = finished_job(:completed, %{status: "completed", response_status: 201, body: "made"})
    body = stream_body(job)

    assert body =~ "event: queued"
    assert body =~ "event: dispatching"
    assert [_, data] = Regex.run(~r/event: completed\ndata: (.*)\n/, body)
    assert %{"response_status" => 201, "body" => "made", "job_id" => id} = Jason.decode!(data)
    assert id == job.id
  end

  test "a failed job's stream sends the failure and ends" do
    job = finished_job(:failed, %{status: "failed", reason: "upstream returned 400"})
    body = stream_body(job)

    assert [_, data] = Regex.run(~r/event: failed\ndata: (.*)\n/, body)
    assert %{"reason" => "upstream returned 400"} = Jason.decode!(data)
  end
end
