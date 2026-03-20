defmodule ExplorerDuckDB.ArrowVTabTest do
  @moduledoc """
  Tests for Arrow vtab zero-copy table registration.
  Isolates which data types work and which don't.
  """
  use ExUnit.Case, async: false

  require Explorer.DataFrame
  alias Explorer.Series
  alias Explorer.DataFrame

  setup do
    Explorer.Backend.put(ExplorerDuckDB)
    ExplorerDuckDB.open(:memory)
    :ok
  end

  # We need to add a NIF that uses Arrow vtab directly for testing
  # For now, test the full pipeline with different data types to
  # see which ones actually work through register_df -> compute

  describe "data type roundtrips through temp tables" do
    test "integers s64" do
      df = DataFrame.new(x: [1, 2, 3, 4, 5])
      result = df |> DataFrame.head(3) |> DataFrame.pull("x") |> Series.to_list()
      assert result == [1, 2, 3]
    end

    test "floats f64" do
      df = DataFrame.new(x: [1.1, 2.2, 3.3])
      result = df |> DataFrame.head(2) |> DataFrame.pull("x") |> Series.to_list()
      assert_in_delta hd(result), 1.1, 0.001
    end

    test "strings" do
      df = DataFrame.new(x: ["hello", "world", "foo"])
      result = df |> DataFrame.head(2) |> DataFrame.pull("x") |> Series.to_list()
      assert result == ["hello", "world"]
    end

    test "booleans" do
      df = DataFrame.new(x: [true, false, true])
      result = df |> DataFrame.head(2) |> DataFrame.pull("x") |> Series.to_list()
      assert result == [true, false]
    end

    test "with nils" do
      df = DataFrame.new(x: [1, nil, 3, nil, 5])
      result = df |> DataFrame.head(5) |> DataFrame.pull("x") |> Series.to_list()
      assert result == [1, nil, 3, nil, 5]
    end

    test "mixed types multi-column" do
      df = DataFrame.new(
        ints: [1, 2, 3],
        floats: [1.1, 2.2, 3.3],
        strings: ["a", "b", "c"],
        bools: [true, false, true]
      )
      result = df |> DataFrame.head(2)
      assert DataFrame.n_rows(result) == 2
      assert Series.to_list(DataFrame.pull(result, "ints")) == [1, 2]
      assert Series.to_list(DataFrame.pull(result, "strings")) == ["a", "b"]
    end

    test "10k rows" do
      df = DataFrame.new(x: Enum.to_list(1..10_000))
      result = df |> DataFrame.head(5) |> DataFrame.pull("x") |> Series.to_list()
      assert result == [1, 2, 3, 4, 5]
    end

    test "50k rows" do
      df = DataFrame.new(x: Enum.to_list(1..50_000))
      result = df |> DataFrame.tail(3) |> DataFrame.pull("x") |> Series.to_list()
      assert result == [49_998, 49_999, 50_000]
    end

    test "from DuckDB query result (may have special types)" do
      df = ExplorerDuckDB.query("SELECT i AS x, i * 2.0 AS y, i > 500 AS big FROM generate_series(1, 1000) t(i)")
      result = df |> DataFrame.filter_with(fn row -> Series.greater(row["x"], 995) end)
      assert DataFrame.n_rows(result) == 5
    end

    test "DuckDB query with aggregations (Decimal128 type)" do
      df = ExplorerDuckDB.query("SELECT SUM(i) AS total FROM generate_series(1, 100) t(i)")
      total = df |> DataFrame.pull("total") |> Series.to_list() |> hd()
      assert total == 5050
    end

    test "chained operations on 10k rows" do
      df = DataFrame.new(
        x: Enum.to_list(1..10_000),
        y: Enum.map(1..10_000, &(&1 * 2))
      )

      result =
        df
        |> DataFrame.filter_with(fn row -> Series.greater(row["x"], 9990) end)
        |> DataFrame.mutate_with(fn row -> [z: Series.add(row["x"], row["y"])] end)
        |> DataFrame.sort_by(desc: z)
        |> DataFrame.head(3)

      assert DataFrame.n_rows(result) == 3
      xs = Series.to_list(DataFrame.pull(result, "x"))
      assert hd(xs) == 10_000
    end
  end
end
