defmodule ExplorerDuckDB.IOOptionsTest do
  use ExUnit.Case, async: true

  require Explorer.DataFrame
  alias Explorer.DataFrame
  alias Explorer.Series

  setup do
    Explorer.Backend.put(ExplorerDuckDB)
    :ok
  end

  describe "from_csv options" do
    @tag :tmp_dir
    test "max_rows limits rows read", %{tmp_dir: tmp_dir} do
      path = Path.join(tmp_dir, "data.csv")
      df = DataFrame.new(x: Enum.to_list(1..100))
      DataFrame.to_csv(df, path)

      loaded = DataFrame.from_csv!(path, max_rows: 10)
      assert DataFrame.n_rows(loaded) == 10
    end

    @tag :tmp_dir
    test "custom delimiter", %{tmp_dir: tmp_dir} do
      path = Path.join(tmp_dir, "tab.csv")
      File.write!(path, "a\tb\n1\t2\n3\t4\n")

      loaded = DataFrame.from_csv!(path, delimiter: "\t")
      assert DataFrame.n_rows(loaded) == 2
      assert "a" in DataFrame.names(loaded)
    end

    @tag :tmp_dir
    test "column selection", %{tmp_dir: tmp_dir} do
      path = Path.join(tmp_dir, "cols.csv")
      df = DataFrame.new(a: [1, 2], b: [3, 4], c: [5, 6])
      DataFrame.to_csv(df, path)

      loaded = DataFrame.from_csv!(path, columns: ["a", "c"])
      assert DataFrame.names(loaded) == ["a", "c"]
      assert DataFrame.n_rows(loaded) == 2
    end
  end

  describe "from_parquet options" do
    @tag :tmp_dir
    test "max_rows limits rows read", %{tmp_dir: tmp_dir} do
      path = Path.join(tmp_dir, "data.parquet")
      df = DataFrame.new(x: Enum.to_list(1..100))
      DataFrame.to_parquet(df, path)

      loaded = DataFrame.from_parquet!(path, max_rows: 10)
      assert DataFrame.n_rows(loaded) == 10
    end

    @tag :tmp_dir
    test "column selection", %{tmp_dir: tmp_dir} do
      path = Path.join(tmp_dir, "cols.parquet")
      df = DataFrame.new(a: [1, 2], b: [3, 4], c: [5, 6])
      DataFrame.to_parquet(df, path)

      loaded = DataFrame.from_parquet!(path, columns: ["a", "c"])
      assert DataFrame.names(loaded) == ["a", "c"]
    end
  end

  describe "application startup" do
    test "application starts successfully" do
      # The application should already be started by mix test
      assert {:ok, _} = Application.ensure_all_started(:explorer_duckdb_backend)
    end
  end
end
