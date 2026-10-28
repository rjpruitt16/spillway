# Copyright 2026 Rahmi Pruitt
# SPDX-License-Identifier: Apache-2.0

defmodule Spillway.UrlPolicy do
  @moduledoc """
  Which upstream hosts Spillway may call. Mirrors Aquifer's urlpolicy.
  """

  @doc """
  Whether `host` is in SPILLWAY_ALLOWED_URL_DOMAINS (comma-separated; a domain
  also allows its subdomains). An empty list allows every host.
  """
  def allowed_host?(host) when is_binary(host) do
    case System.get_env("SPILLWAY_ALLOWED_URL_DOMAINS", "") do
      "" ->
        true

      allowlist ->
        Enum.any?(String.split(allowlist, ","), fn allowed ->
          allowed = String.trim(allowed)
          allowed != "" and (host == allowed or String.ends_with?(host, "." <> allowed))
        end)
    end
  end

  def allowed_host?(_host), do: System.get_env("SPILLWAY_ALLOWED_URL_DOMAINS", "") == ""

  @doc "Whether `url`'s host is allowed. A URL with no host is allowed only when the list is empty."
  def allowed_url?(url) when is_binary(url), do: allowed_host?(URI.parse(url).host)
end
