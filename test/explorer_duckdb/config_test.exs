defmodule ExplorerDuckDB.ConfigTest do
  use ExUnit.Case

  require Explorer.DataFrame
  alias Explorer.DataFrame
  alias Explorer.Series

  describe "config-driven database" do
    @tag :tmp_dir
    test "uses configured database path", %{tmp_dir: tmp_dir} do
      path = Path.join(tmp_dir, "config_test.duckdb")

      # Set config
      Application.put_env(:explorer_duckdb_backend, :database, path)

      # Reset process db to force re-read from config
      Process.delete(:explorer_duckdb_db)
      Explorer.Backend.put(ExplorerDuckDB)

      # Create data
      ExplorerDuckDB.execute("CREATE TABLE config_test (x INTEGER)")
      ExplorerDuckDB.execute("INSERT INTO config_test VALUES (1), (2), (3)")

      df = ExplorerDuckDB.query("SELECT * FROM config_test ORDER BY x")
      assert Series.to_list(DataFrame.pull(df, "x")) == [1, 2, 3]

      # Reset config
      Application.put_env(:explorer_duckdb_backend, :database, :memory)
      Process.delete(:explorer_duckdb_db)
    end

    test "defaults to in-memory when no config" do
      Application.delete_env(:explorer_duckdb_backend, :database)
      Process.delete(:explorer_duckdb_db)
      Explorer.Backend.put(ExplorerDuckDB)

      df = DataFrame.new(x: [1, 2, 3])
      assert DataFrame.n_rows(df) == 3

      # Cleanup
      Process.delete(:explorer_duckdb_db)
    end
  end

  describe "query builder CTE format" do
    test "chained operations produce readable CTEs" do
      Explorer.Backend.put(ExplorerDuckDB)
      ExplorerDuckDB.open(:memory)

      df = DataFrame.new(x: Enum.to_list(1..20), y: Enum.map(1..20, &(&1 * 10)))

      # This chain goes through the lazy pipeline which uses CTEs
      result =
        df
        |> DataFrame.filter_with(fn row -> Series.greater(row["x"], 5) end)
        |> DataFrame.mutate_with(fn row -> [z: Series.add(row["x"], row["y"])] end)
        |> DataFrame.sort_by(desc: z)
        |> DataFrame.head(3)

      assert DataFrame.n_rows(result) == 3
      # Top 3 by z descending -- z = x + y, so biggest is x=20, y=200, z=220
      assert Series.to_list(DataFrame.pull(result, "x")) == [20, 19, 18]
    end
  end
end
