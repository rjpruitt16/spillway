# Copyright 2026 Rahmi Pruitt
# SPDX-License-Identifier: Apache-2.0

defmodule SpillwayWeb.Plugs.Fullness do
  @moduledoc """
  Puts `x-aqueduct-fullness` (0-100, see Spillway.Admission.fullness/0) on
  every response, including the 429s and 503s this node sheds with, so
  whatever is in front of it can decide when to stop sending it new work.
  Runs ahead of InFlightLimit for exactly that reason. Also puts this
  node's load (Spillway.Load) as `x-aqueduct-http-users`,
  `x-aqueduct-http-capacity`, `x-aqueduct-ws-sessions` and
  `x-aqueduct-ws-capacity`.
  """

  import Plug.Conn

  def init(opts), do: opts

  def call(conn, _opts) do
    conn =
      put_resp_header(
        conn,
        "x-aqueduct-fullness",
        Integer.to_string(Spillway.Admission.fullness())
      )

    case Spillway.Load.current() do
      nil ->
        conn

      load ->
        conn
        |> put_resp_header("x-aqueduct-http-users", Integer.to_string(load.http_users))
        |> put_resp_header("x-aqueduct-http-capacity", Integer.to_string(load.http_capacity))
        |> put_resp_header("x-aqueduct-ws-sessions", Integer.to_string(load.ws_sessions))
        |> put_resp_header("x-aqueduct-ws-capacity", Integer.to_string(load.ws_capacity))
    end
  end
end
