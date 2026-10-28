# Copyright 2026 Rahmi Pruitt
# SPDX-License-Identifier: Apache-2.0

defmodule Spillway.NodeId do
  @moduledoc """
  Names this node to the backends it dispatches to (X-Aqueduct-Node-Id), so a
  backend can count how many nodes are sending to it and divide its pacing
  budget between them. Mirrors Aquifer's nodeid.
  """

  @key {__MODULE__, :id}

  @doc "The first of SPILLWAY_NODE_ID, FLY_MACHINE_ID and the host name that is set."
  def get do
    case :persistent_term.get(@key, nil) do
      nil ->
        id = resolve(&System.get_env/1, &:inet.gethostname/0)
        :persistent_term.put(@key, id)
        id

      id ->
        id
    end
  end

  @doc "get/0 without the caching, for tests."
  def resolve(getenv, hostname) do
    from_env =
      Enum.find_value(["SPILLWAY_NODE_ID", "FLY_MACHINE_ID"], fn name ->
        case getenv.(name) do
          value when is_binary(value) ->
            if String.trim(value) != "", do: String.trim(value)

          _ ->
            nil
        end
      end)

    from_env ||
      case hostname.() do
        {:ok, host} when host not in [~c"", ""] -> to_string(host)
        _ -> "unknown"
      end
  end
end
