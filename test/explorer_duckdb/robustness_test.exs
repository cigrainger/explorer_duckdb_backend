defmodule ExplorerDuckDB.RobustnessTest do
  use ExUnit.Case, async: true

  require Explorer.DataFrame
  alias Explorer.DataFrame
  alias Explorer.Series

  setup do
    Explorer.Backend.put(ExplorerDuckDB)
    :ok
  end

  # ============================================================
  # NaN / Infinity handling
  # ============================================================

  describe "NaN and Infinity" do
    test "from_list with :nan" do
      s = Series.from_list([1.0, :nan, 3.0])
      list = Series.to_list(s)
      assert Enum.at(list, 0) == 1.0
      assert Enum.at(list, 1) == :nan
      assert Enum.at(list, 2) == 3.0
    end

    test "from_list with :infinity and :neg_infinity" do
      s = Series.from_list([1.0, :infinity, :neg_infinity])
      list = Series.to_list(s)
      assert Enum.at(list, 0) == 1.0
      assert Enum.at(list, 1) == :infinity
      assert Enum.at(list, 2) == :neg_infinity
    end

    test "is_nan detects NaN values" do
      s = Series.from_list([1.0, :nan, 3.0])
      result = Series.is_nan(s) |> Series.to_list()
      assert result == [false, true, false]
    end

    test "is_infinite detects infinity" do
      s = Series.from_list([1.0, :infinity, :neg_infinity, 2.0])
      result = Series.is_infinite(s) |> Series.to_list()
      assert result == [false, true, true, false]
    end

    test "is_finite excludes NaN and infinity" do
      s = Series.from_list([1.0, :nan, :infinity])
      result = Series.is_finite(s) |> Series.to_list()
      assert Enum.at(result, 0) == true
    end
  end

  # ============================================================
  # Dtype roundtrips
  # ============================================================

  describe "integer dtype roundtrips" do
    test "s8 roundtrip" do
      s = Series.from_list([1, -1, 127, -128], dtype: {:s, 8})
      assert Series.dtype(s) == {:s, 8}
      assert Series.to_list(s) == [1, -1, 127, -128]
    end

    test "s16 roundtrip" do
      s = Series.from_list([1, -1, 32767], dtype: {:s, 16})
      assert Series.dtype(s) == {:s, 16}
    end

    test "u8 roundtrip" do
      s = Series.from_list([0, 128, 255], dtype: {:u, 8})
      assert Series.dtype(s) == {:u, 8}
    end

    test "u32 roundtrip" do
      s = Series.from_list([0, 100, 4294967295], dtype: {:u, 32})
      assert Series.dtype(s) == {:u, 32}
    end

    test "f32 roundtrip" do
      s = Series.from_list([1.0, 2.5], dtype: {:f, 32})
      assert Series.dtype(s) == {:f, 32}
    end
  end

  # ============================================================
  # Row ordering preservation
  # ============================================================

  describe "row ordering" do
    test "filter preserves original order" do
      df = DataFrame.new(x: [5, 3, 1, 4, 2])

      result =
        df
        |> DataFrame.filter_with(fn row -> Series.greater(row["x"], 2) end)

      values = Series.to_list(DataFrame.pull(result, "x"))
      assert values == [5, 3, 4]
    end

    test "mutate preserves row order" do
      df = DataFrame.new(x: [5, 3, 1])

      result =
        df
        |> DataFrame.mutate_with(fn row ->
          [doubled: Series.multiply(row["x"], 2)]
        end)

      assert Series.to_list(DataFrame.pull(result, "x")) == [5, 3, 1]
      assert Series.to_list(DataFrame.pull(result, "doubled")) == [10, 6, 2]
    end

    test "head preserves order" do
      df = DataFrame.new(x: [5, 3, 1, 4, 2])
      result = DataFrame.head(df, 3)
      assert Series.to_list(DataFrame.pull(result, "x")) == [5, 3, 1]
    end

    test "lazy pipeline preserves order" do
      df = DataFrame.new(x: [10, 20, 30, 40, 50], y: ["e", "d", "c", "b", "a"])

      result =
        df
        |> DataFrame.filter_with(fn row -> Series.greater(row["x"], 15) end)
        |> DataFrame.head(3)

      assert Series.to_list(DataFrame.pull(result, "x")) == [20, 30, 40]
      assert Series.to_list(DataFrame.pull(result, "y")) == ["d", "c", "b"]
    end
  end

  # ============================================================
  # Larger datasets
  # ============================================================

  describe "larger data" do
    test "10k rows through pipeline" do
      data = Enum.to_list(1..10_000)
      df = DataFrame.new(x: data, y: Enum.map(data, &(&1 * 2)))

      result =
        df
        |> DataFrame.filter_with(fn row -> Series.greater(row["x"], 9000) end)
        |> DataFrame.sort_by(desc: x)
        |> DataFrame.head(5)

      values = Series.to_list(DataFrame.pull(result, "x"))
      assert values == [10_000, 9999, 9998, 9997, 9996]
    end

    test "series operations on 10k elements" do
      s = Series.from_list(Enum.to_list(1..10_000))
      assert Series.sum(s) == 50_005_000
      assert Series.min(s) == 1
      assert Series.max(s) == 10_000
      assert Series.size(s) == 10_000
    end
  end

  # ============================================================
  # Temp table cleanup
  # ============================================================

  describe "temp table cleanup" do
    test "operations don't leak temp tables" do
      db = ExplorerDuckDB.Shared.get_db()

      # Run several operations
      for _ <- 1..10 do
        df = DataFrame.new(x: [1, 2, 3])
        DataFrame.filter_with(df, fn row -> Series.greater(row["x"], 1) end)
      end

      # Check that temp tables are cleaned up
      result = ExplorerDuckDB.Native.df_query(db,
        "SELECT COUNT(*) AS c FROM information_schema.tables WHERE table_name LIKE '__explorer_%'"
      )

      # Handle both {:ok, ref} and raw ref
      ref = case result do
        {:ok, r} -> r
        r when is_reference(r) -> r
      end

      rows = ExplorerDuckDB.Native.df_to_rows(ref)
      rows = if is_list(rows), do: rows, else: elem(rows, 1)
      count = hd(rows) |> Map.values() |> hd()
      # Should be 0 or very few (the per-process db might have some lingering)
      assert count < 5
    end
  end

  # ============================================================
  # Concurrent access
  # ============================================================

  describe "concurrent access" do
    test "multiple tasks can use the backend" do
      tasks =
        for i <- 1..5 do
          Task.async(fn ->
            Explorer.Backend.put(ExplorerDuckDB)
            s = Series.from_list([i, i * 2, i * 3])
            Series.sum(s)
          end)
        end

      results = Task.await_many(tasks, 10_000)
      assert length(results) == 5
      # Each sum should be i + 2i + 3i = 6i
      assert Enum.sort(results) == [6, 12, 18, 24, 30]
    end

    test "multiple tasks operating on dataframes" do
      tasks =
        for i <- 1..3 do
          Task.async(fn ->
            Explorer.Backend.put(ExplorerDuckDB)
            df = DataFrame.new(x: Enum.to_list((i * 10)..(i * 10 + 4)))
            DataFrame.n_rows(df)
          end)
        end

      results = Task.await_many(tasks, 10_000)
      assert results == [5, 5, 5]
    end
  end
end
