# Copyright 2026 Rahmi Pruitt
# SPDX-License-Identifier: Apache-2.0

defmodule Spillway.FinishWebhookTest do
  @moduledoc """
  A finished job and the webhook delivery that reports it are stored in one
  write (IdempotentStore.finish/3). Marking the job finished first and
  storing the delivery second lost the webhook when the node was killed in
  between: the job no longer looked recoverable and nothing had stored its
  delivery (soak run 3l lost 7-26 per kill -9).
  """

  use ExUnit.Case, async: false

  alias Spillway.{IdempotentStore, Job}

  defp stored_job do
    stamp = System.unique_integer([:positive])

    job = %Job{
      id: "finish-#{stamp}",
      user_id: "finish-user",
      idempotent_key: "finish-key-#{stamp}",
      url: "https://example.com/upstream",
      method: "POST",
      headers: %{},
      body: nil,
      webhook_url: "https://example.com/callback",
      status: :queued,
      created_at: System.system_time(:millisecond)
    }

    :ok = IdempotentStore.check_or_insert(job)
    job
  end

  defp delivery_for(job, status) do
    Job.new_webhook_delivery(job.id, job.user_id, job.webhook_url, %{
      job_id: job.id,
      status: status
    })
  end

  defp recoverable_ids, do: IdempotentStore.recoverable_jobs() |> Enum.map(& &1.id)

  test "a node killed right after a job finishes still recovers the job's webhook" do
    job = stored_job()
    delivery = delivery_for(job, "completed")

    # The store write is all that happens before the hand-off to the webhook
    # queue, so stopping here is what a kill -9 at that moment leaves behind.
    assert IdempotentStore.finish(job.id, :completed, delivery) == :inserted

    assert IdempotentStore.get_status(job.id) == "completed"
    ids = recoverable_ids()
    refute job.id in ids
    assert delivery.id in ids
  end

  test "a failed job's webhook is stored with the failure" do
    job = stored_job()
    delivery = delivery_for(job, "failed")

    assert IdempotentStore.finish(job.id, :failed, delivery) == :inserted
    assert IdempotentStore.get_status(job.id) == "failed"
    assert delivery.id in recoverable_ids()
  end

  test "a job's webhook is stored once, even if it finishes again" do
    job = stored_job()
    first = delivery_for(job, "completed")
    again = delivery_for(job, "completed")

    assert IdempotentStore.finish(job.id, :completed, first) == :inserted
    assert IdempotentStore.finish(job.id, :completed, again) == :duplicate

    ids = recoverable_ids()
    assert first.id in ids
    refute again.id in ids
  end

  test "a job with no webhook is just marked finished" do
    job = stored_job()

    assert IdempotentStore.finish(job.id, :completed, nil) == :none
    assert IdempotentStore.get_status(job.id) == "completed"
  end
end
