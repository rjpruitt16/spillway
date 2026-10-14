# Copyright 2026 Rahmi Pruitt
# SPDX-License-Identifier: Apache-2.0

defmodule Spillway.JobOptionalWebhookTest do
  use ExUnit.Case, async: true

  alias Spillway.Job

  test "a user job may omit webhook_url without becoming an internal webhook delivery" do
    assert {:ok, job} =
             Job.new(%{
               "user_id" => "user-1",
               "idempotent_key" => "event-1",
               "url" => "https://example.com/webhook",
               "method" => "POST"
             })

    assert job.webhook_url == nil
    refute Job.webhook_delivery_job?(job)
  end

  test "internal webhook delivery remains explicit" do
    job = Job.new_webhook_delivery("job-1", "user-1", "https://example.com/result", %{})
    assert Job.webhook_delivery_job?(job)
  end

  test "old persisted internal jobs retain the empty-URL fallback" do
    assert Job.webhook_delivery_job?(%Job{kind: nil, webhook_url: ""})
  end
end
