# Copyright 2026 Rahmi Pruitt
# SPDX-License-Identifier: Apache-2.0

defmodule SpillwayWeb.MetricsController do
  use SpillwayWeb, :controller

  @doc "GET /metrics: Prometheus text format, or 404 when another metrics adapter is configured."
  def index(conn, _params) do
    if Spillway.Metrics.adapter() == Spillway.Metrics.Prometheus do
      conn
      |> put_resp_content_type("text/plain; version=0.0.4")
      |> send_resp(200, Spillway.Metrics.Prometheus.render())
    else
      send_resp(conn, 404, "")
    end
  end
end
