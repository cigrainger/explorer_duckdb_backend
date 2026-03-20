defmodule ExplorerDuckDB.StressTest do
  use ExUnit.Case, async: false
  use ExUnitProperties

  require Explorer.DataFrame
  alias Explorer.DataFrame
  alias Explorer.Series

  setup do
    Explorer.Backend.put(ExplorerDuckDB)
    ExplorerDuckDB.open(:memory)
    :ok
  end

  # ============================================================
  # Large dataset stress tests
  # ============================================================

  describe "large datasets" do
    test "100k rows through filter + sort + head pipeline" do
      df = DataFrame.new(x: Enum.to_list(1..100_000))

      result =
        df
        |> DataFrame.filter_with(fn row -> Series.greater(row["x"], 90_000) end)
        |> DataFrame.sort_by(desc: x)
        |> DataFrame.head(5)

      assert Series.to_list(DataFrame.pull(result, "x")) == [100_000, 99_999, 99_998, 99_997, 99_996]
    end

    test "100k rows with multiple columns" do
      df = DataFrame.new(
        a: Enum.to_list(1..100_000),
        b: Enum.map(1..100_000, &(&1 * 2)),
        c: Enum.map(1..100_000, &to_string/1)
      )

      assert DataFrame.n_rows(df) == 100_000
      assert DataFrame.names(df) |> length() == 3
    end

    test "chained mutations on large data" do
      df = DataFrame.new(x: Enum.to_list(1..50_000))

      result =
        df
        |> DataFrame.mutate_with(fn row -> [y: Series.multiply(row["x"], 2)] end)
        |> DataFrame.filter_with(fn row -> Series.greater(row["y"], 50_000) end)
        |> DataFrame.head(3)

      assert DataFrame.n_rows(result) == 3
    end
  end

  # ============================================================
  # Streaming tests
  # ============================================================

  describe "streaming" do
    test "stream_query returns all rows" do
      batches =
        ExplorerDuckDB.stream_query("SELECT * FROM generate_series(1, 10000) t(x)")
        |> Enum.to_list()

      total = Enum.map(batches, &DataFrame.n_rows/1) |> Enum.sum()
      assert total == 10_000
    end

    test "stream can be lazily consumed" do
      first_batch =
        ExplorerDuckDB.stream_query("SELECT * FROM generate_series(1, 10000) t(x)")
        |> Enum.take(1)
        |> hd()

      assert DataFrame.n_rows(first_batch) == 2048
    end
  end

  # ============================================================
  # SQL preview and explain
  # ============================================================

  describe "sql_preview" do
    test "shows CTE SQL for lazy pipeline" do
      df = DataFrame.new(x: [1, 2, 3])
      lazy = DataFrame.lazy(df)
      lazy = DataFrame.filter_with(lazy, fn row -> Series.greater(row["x"], 1) end)
      lazy = DataFrame.sort_by(lazy, asc: x)

      sql = ExplorerDuckDB.sql_preview(lazy)
      assert sql =~ "WITH"
      assert sql =~ "WHERE"
      assert sql =~ "ORDER BY"
    end

    test "shows info for eager DataFrame" do
      df = DataFrame.new(x: [1, 2, 3])
      result = ExplorerDuckDB.sql_preview(df)
      assert result =~ "Eager"
    end
  end

  describe "explain" do
    test "returns query plan" do
      plan = ExplorerDuckDB.explain("SELECT * FROM generate_series(1, 100) t(x) WHERE x > 50")
      assert is_binary(plan)
      assert String.length(plan) > 0
    end
  end

  # ============================================================
  # Error recovery
  # ============================================================

  describe "error recovery" do
    test "invalid SQL returns error, doesn't crash" do
      assert_raise RuntimeError, fn ->
        ExplorerDuckDB.query("INVALID SQL SYNTAX HERE!!!")
      end

      # Backend should still work after error
      df = DataFrame.new(x: [1, 2, 3])
      assert DataFrame.n_rows(df) == 3
    end

    test "query on non-existent table returns error" do
      assert_raise RuntimeError, fn ->
        ExplorerDuckDB.query("SELECT * FROM nonexistent_table_xyz")
      end
    end

    test "operations work after failed query" do
      try do
        ExplorerDuckDB.query("BAD SQL")
      rescue
        _ -> :ok
      end

      s = Series.from_list([1, 2, 3])
      assert Series.sum(s) == 6
    end
  end

  # ============================================================
  # IPC round-trip through lazy pipeline
  # ============================================================

  describe "IPC with lazy" do
    test "lazy result can be serialized to IPC" do
      df = DataFrame.new(x: [5, 3, 1, 4, 2])

      result =
        df
        |> DataFrame.sort_by(asc: x)
        |> DataFrame.head(3)

      {:ok, ipc} = DataFrame.dump_ipc(result)
      {:ok, loaded} = DataFrame.load_ipc(ipc)

      assert Series.to_list(DataFrame.pull(loaded, "x")) == [1, 2, 3]
    end
  end

  # ============================================================
  # Property tests for DataFrame operations
  # ============================================================

  describe "DataFrame properties" do
    property "IPC round-trip preserves data" do
      check all(list <- list_of(integer(-1000..1000), min_length: 1, max_length: 50)) do
        df = DataFrame.new(x: list)
        {:ok, ipc} = DataFrame.dump_ipc(df)
        {:ok, loaded} = DataFrame.load_ipc(ipc)
        assert Series.to_list(DataFrame.pull(loaded, "x")) == list
      end
    end

    property "IPC stream round-trip preserves data" do
      check all(list <- list_of(integer(), min_length: 1, max_length: 50)) do
        df = DataFrame.new(x: list)
        {:ok, ipc} = DataFrame.dump_ipc_stream(df)
        {:ok, loaded} = DataFrame.load_ipc_stream(ipc)
        assert Series.to_list(DataFrame.pull(loaded, "x")) == list
      end
    end

    property "distinct count <= original count" do
      check all(list <- list_of(integer(1..10), min_length: 1, max_length: 50)) do
        df = DataFrame.new(x: list)
        result = DataFrame.distinct(df)
        assert DataFrame.n_rows(result) <= DataFrame.n_rows(df)
      end
    end

    property "drop_nil count <= original count" do
      check all(
              list <- list_of(one_of([integer(), constant(nil)]), min_length: 1, max_length: 50)
            ) do
        df = DataFrame.new(x: list)
        result = DataFrame.drop_nil(df)
        assert DataFrame.n_rows(result) <= DataFrame.n_rows(df)
      end
    end
  end

  # ============================================================
  # Concurrent operations
  # ============================================================

  describe "concurrent stress" do
    test "10 concurrent tasks creating and querying DataFrames" do
      tasks =
        for i <- 1..10 do
          Task.async(fn ->
            Explorer.Backend.put(ExplorerDuckDB)
            df = DataFrame.new(x: Enum.to_list(1..1000), id: List.duplicate(i, 1000))

            result =
              df
              |> DataFrame.filter_with(fn row -> Series.greater(row["x"], 500) end)
              |> DataFrame.head(5)

            {i, DataFrame.n_rows(result)}
          end)
        end

      results = Task.await_many(tasks, 30_000)
      assert length(results) == 10
      assert Enum.all?(results, fn {_, n} -> n == 5 end)
    end
  end
end
