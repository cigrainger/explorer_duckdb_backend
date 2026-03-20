defmodule ExplorerDuckDBBackend.MixProject do
  use Mix.Project

  @version "0.1.0-dev"

  def project do
    [
      app: :explorer_duckdb_backend,
      name: "ExplorerDuckDBBackend",
      description: "DuckDB backend for the Explorer data analysis library",
      version: @version,
      elixir: "~> 1.14",
      elixirc_paths: elixirc_paths(Mix.env()),
      deps: deps(),
      aliases: [
        "rust.lint": [
          "cmd cargo clippy --manifest-path=native/explorer_duckdb/Cargo.toml -- -Dwarnings"
        ],
        "rust.fmt": ["cmd cargo fmt --manifest-path=native/explorer_duckdb/Cargo.toml --all"]
      ]
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp elixirc_paths(:test), do: ~w(lib test/support)
  defp elixirc_paths(_), do: ~w(lib)

  defp deps do
    [
      {:explorer, "~> 0.11.1"},
      {:rustler, "~> 0.36.0", runtime: false},
      {:stream_data, "~> 1.1", only: [:dev, :test]}
    ]
  end
end
