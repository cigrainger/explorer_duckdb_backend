defmodule ExplorerDuckDB.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children =
      case Application.get_env(:explorer_duckdb_backend, :connection) do
        nil ->
          []

        opts when is_list(opts) ->
          # Start a named connection from config
          # config :explorer_duckdb_backend, connection: [path: "my.duckdb", name: :default_db]
          [{ExplorerDuckDB.Connection, opts}]
      end

    opts = [strategy: :one_for_one, name: ExplorerDuckDB.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
