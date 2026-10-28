# Copyright 2026 Rahmi Pruitt
# SPDX-License-Identifier: Apache-2.0

defmodule Spillway.UrlPolicyTest do
  # Sets SPILLWAY_ALLOWED_URL_DOMAINS, which every job reads.
  use ExUnit.Case, async: false

  alias Spillway.Job

  setup do
    on_exit(fn -> System.delete_env("SPILLWAY_ALLOWED_URL_DOMAINS") end)
  end

  defp job(url),
    do: Job.new(%{"user_id" => "u", "idempotent_key" => "k", "url" => url, "method" => "POST"})

  test "with no allowlist every host is allowed" do
    assert {:ok, _} = job("https://anywhere.example/work")
  end

  test "a job outside the allowlist is refused; the domain and its subdomains are allowed" do
    System.put_env("SPILLWAY_ALLOWED_URL_DOMAINS", "example.com, api.openai.com")

    assert {:ok, _} = job("https://example.com/work")
    assert {:ok, _} = job("https://api.example.com/work")
    assert {:ok, _} = job("https://api.openai.com/v1")

    assert {:error, "url domain is not in the configured allowlist" <> _} =
             job("https://evil.com/")

    assert {:error, _} = job("https://notexample.com/")
    assert {:error, _} = job("not a url")
  end

  test "a pool-routed job has no url to check" do
    System.put_env("SPILLWAY_ALLOWED_URL_DOMAINS", "example.com")

    assert {:ok, _} =
             Job.new(%{
               "user_id" => "u",
               "idempotent_key" => "k",
               "pool_id" => "p",
               "method" => "POST"
             })
  end
end
