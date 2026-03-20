defmodule ExplorerDuckDB.LazyFrameTest do
  use ExUnit.Case, async: true

  require Explorer.DataFrame
  alias Explorer.DataFrame
  alias Explorer.Series

  setup do
    Explorer.Backend.put(ExplorerDuckDB)
    :ok
  end

  describe "lazy pipeline" do
    test "chained operations execute as single query" do
      df = DataFrame.new(x: [5, 3, 1, 4, 2], y: ["e", "c", "a", "d", "b"])

      # This chain should produce a single SQL query through the lazy pipeline
      result =
        df
        |> DataFrame.filter_with(fn row -> Series.greater(row["x"], 2) end)
        |> DataFrame.sort_by(asc: x)
        |> DataFrame.select(["x"])

      assert DataFrame.n_rows(result) == 3
      assert Series.to_list(DataFrame.pull(result, "x")) == [3, 4, 5]
    end

    test "filter then mutate then select" do
      df = DataFrame.new(a: [1, 2, 3, 4, 5], b: [10, 20, 30, 40, 50])

      result =
        df
        |> DataFrame.filter_with(fn row -> Series.greater(row["a"], 2) end)
        |> DataFrame.mutate_with(fn row -> [total: Series.add(row["a"], row["b"])] end)
        |> DataFrame.select(["a", "total"])

      assert DataFrame.names(result) == ["a", "total"]
      assert Series.to_list(DataFrame.pull(result, "a")) == [3, 4, 5]
      assert Series.to_list(DataFrame.pull(result, "total")) == [33, 44, 55]
    end

    test "explicit lazy/compute roundtrip" do
      df = DataFrame.new(x: [3, 1, 2])

      lazy_df = DataFrame.lazy(df)
      # The lazy df should have LazyFrame as its data
      assert lazy_df.data.__struct__ == ExplorerDuckDB.LazyFrame

      # Computing should return an eager dataframe
      eager_df = DataFrame.compute(lazy_df)
      assert eager_df.data.__struct__ == ExplorerDuckDB.DataFrame
      assert Series.to_list(DataFrame.pull(eager_df, "x")) == [3, 1, 2]
    end

    test "multiple filters compose" do
      df = DataFrame.new(x: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10])

      result =
        df
        |> DataFrame.filter_with(fn row -> Series.greater(row["x"], 3) end)
        |> DataFrame.filter_with(fn row -> Series.less(row["x"], 8) end)

      assert Series.to_list(DataFrame.pull(result, "x")) == [4, 5, 6, 7]
    end

    test "head after filter" do
      df = DataFrame.new(x: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10])

      result =
        df
        |> DataFrame.filter_with(fn row -> Series.greater(row["x"], 5) end)
        |> DataFrame.head(3)

      assert DataFrame.n_rows(result) == 3
      assert Series.to_list(DataFrame.pull(result, "x")) == [6, 7, 8]
    end

    test "sort then slice" do
      df = DataFrame.new(x: [5, 3, 1, 4, 2])

      result =
        df
        |> DataFrame.sort_by(asc: x)
        |> DataFrame.slice(1, 3)

      assert Series.to_list(DataFrame.pull(result, "x")) == [2, 3, 4]
    end
  end
end
