defmodule ExplorerDuckDB.NewlyImplementedTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  require Explorer.DataFrame
  alias Explorer.DataFrame
  alias Explorer.Series

  setup do
    Explorer.Backend.put(ExplorerDuckDB)
    :ok
  end

  # ============================================================
  # re_scan
  # ============================================================

  describe "re_scan" do
    test "extracts all matches" do
      s = Series.from_list(["abc123def456", "no match", "test789"])
      result = Series.re_scan(s, "\\d+")
      list = Series.to_list(result)
      assert is_list(hd(list))
      assert hd(list) == ["123", "456"]
    end
  end

  # ============================================================
  # json_decode
  # ============================================================

  describe "json_decode" do
    test "parses JSON strings" do
      s = Series.from_list([~s({"a": 1}), ~s({"a": 2})])
      result = Series.json_decode(s, :string)
      assert Series.size(result) == 2
    end
  end

  # ============================================================
  # json_path_match
  # ============================================================

  describe "json_path_match" do
    test "extracts JSON values by path" do
      s = Series.from_list([~s({"name": "Alice"}), ~s({"name": "Bob"})])
      result = Series.json_path_match(s, "$.name")
      assert Series.to_list(result) == ["Alice", "Bob"]
    end
  end

  # ============================================================
  # re_replace
  # ============================================================

  describe "re_replace" do
    test "replaces all regex matches" do
      s = Series.from_list(["hello 123 world 456"])
      result = Series.re_replace(s, "\\d+", "NUM")
      assert Series.to_list(result) == ["hello NUM world NUM"]
    end
  end

  # ============================================================
  # count_matches
  # ============================================================

  describe "count_matches" do
    test "counts substring occurrences" do
      s = Series.from_list(["aabaa", "bbb", "ab"])
      result = Series.count_matches(s, "a")
      list = Series.to_list(result)
      # "aabaa" has 4 a's (returns as float due to division)
      assert trunc(Enum.at(list, 0)) == 4
    end
  end

  # ============================================================
  # split
  # ============================================================

  describe "split" do
    test "splits strings by delimiter" do
      s = Series.from_list(["a,b,c", "d,e"])
      result = Series.split(s, ",")
      list = Series.to_list(result)
      assert hd(list) == ["a", "b", "c"]
    end
  end

  # ============================================================
  # cast
  # ============================================================

  describe "cast" do
    test "integer to string" do
      s = Series.from_list([1, 2, 3])
      result = Series.cast(s, :string)
      assert Series.dtype(result) == :string
      assert Series.to_list(result) == ["1", "2", "3"]
    end

    test "string to integer" do
      s = Series.from_list(["1", "2", "3"])
      result = Series.cast(s, {:s, 64})
      assert Series.to_list(result) == [1, 2, 3]
    end
  end

  # ============================================================
  # strftime / strptime (require date data from DuckDB)
  # ============================================================

  describe "strftime" do
    test "formats timestamps" do
      db = ExplorerDuckDB.Shared.get_db()
      ref = ExplorerDuckDB.Native.df_query(db, "SELECT '2024-06-15 14:30:00'::TIMESTAMP AS d")
      ref = case ref do {:ok, r} -> r; r -> r end
      df = ExplorerDuckDB.Shared.create_dataframe!(ref)
      s = DataFrame.pull(df, "d")

      result = Series.strftime(s, "%Y-%m-%d")
      assert hd(Series.to_list(result)) =~ "2024"
    end
  end

  # ============================================================
  # binary_in
  # ============================================================

  describe "binary_in" do
    test "checks membership" do
      s = Series.from_list([1, 2, 3, 4, 5])
      other = Series.from_list([2, 4, 6])
      result = Series.in(s, other)
      assert Series.to_list(result) == [false, true, false, true, false]
    end
  end

  # ============================================================
  # clip
  # ============================================================

  describe "clip" do
    test "clamps values to range" do
      s = Series.from_list([1, 5, 10, 15, 20])
      result = Series.clip(s, 5, 15)
      assert Series.to_list(result) == [5, 5, 10, 15, 15]
    end
  end

  # ============================================================
  # DataFrame property tests
  # ============================================================

  describe "DataFrame properties" do
    property "filter never increases row count" do
      check all(
              list <- list_of(integer(-100..100), min_length: 1, max_length: 50),
              threshold <- integer(-100..100)
            ) do
        df = DataFrame.new(x: list)

        result =
          DataFrame.filter_with(df, fn row ->
            Series.greater(row["x"], threshold)
          end)

        assert DataFrame.n_rows(result) <= DataFrame.n_rows(df)
      end
    end

    property "select preserves row count" do
      check all(list <- list_of(integer(), min_length: 1, max_length: 30)) do
        df = DataFrame.new(a: list, b: Enum.map(list, &(&1 * 2)))
        result = DataFrame.select(df, ["a"])
        assert DataFrame.n_rows(result) == DataFrame.n_rows(df)
        assert DataFrame.names(result) == ["a"]
      end
    end

    property "head + tail covers all rows (no overlap when exact)" do
      check all(
              list <- list_of(integer(), min_length: 2, max_length: 30),
              n <- integer(1..(length(list) - 1))
            ) do
        df = DataFrame.new(x: list)
        head_count = DataFrame.head(df, n) |> DataFrame.n_rows()
        tail_count = DataFrame.tail(df, length(list) - n) |> DataFrame.n_rows()
        assert head_count + tail_count == length(list)
      end
    end

    property "sort then head gives the n smallest" do
      check all(list <- list_of(integer(), min_length: 3, max_length: 30)) do
        df = DataFrame.new(x: list)

        result =
          df
          |> DataFrame.sort_by(asc: x)
          |> DataFrame.head(3)
          |> DataFrame.pull("x")
          |> Series.to_list()

        sorted = Enum.sort(list) |> Enum.take(3)
        assert result == sorted
      end
    end

    property "rename preserves data" do
      check all(list <- list_of(integer(), min_length: 1, max_length: 20)) do
        df = DataFrame.new(original: list)
        renamed = DataFrame.rename(df, original: "new_name")
        assert Series.to_list(DataFrame.pull(renamed, "new_name")) == list
      end
    end

    property "mutate then select original gives same data" do
      check all(list <- list_of(integer(-100..100), min_length: 1, max_length: 20)) do
        df = DataFrame.new(x: list)

        result =
          df
          |> DataFrame.mutate_with(fn row -> [y: Series.multiply(row["x"], 2)] end)
          |> DataFrame.select(["x"])
          |> DataFrame.pull("x")
          |> Series.to_list()

        assert result == list
      end
    end
  end

  # ============================================================
  # Series ordering guarantee
  # ============================================================

  describe "series ordering" do
    test "cumulative_sum preserves order" do
      s = Series.from_list([1, 2, 3, 4, 5])
      result = Series.cumulative_sum(s) |> Series.to_list()
      assert result == [1, 3, 6, 10, 15]
    end

    test "window_sum preserves order" do
      s = Series.from_list([10, 20, 30, 40, 50])
      result = Series.window_sum(s, 2) |> Series.to_list()
      # Window of 2: [10, 30, 50, 70, 90]
      assert Enum.at(result, 0) == 10
      assert Enum.at(result, 1) == 30
    end

    test "fill_missing forward preserves order" do
      s = Series.from_list([1, nil, nil, 4, nil])
      result = Series.fill_missing(s, :forward) |> Series.to_list()
      assert result == [1, 1, 1, 4, 4]
    end

    test "sql_transform preserves insertion order" do
      # Create series with non-sorted values
      s = Series.from_list([50, 10, 40, 20, 30])
      result = Series.abs(s) |> Series.to_list()
      assert result == [50, 10, 40, 20, 30]
    end

    test "round-trip through sql preserves order" do
      s = Series.from_list([5, 3, 1, 4, 2])
      # Cast to float and back -- this goes through sql_transform
      result =
        s
        |> Series.cast({:f, 64})
        |> Series.cast({:s, 64})
        |> Series.to_list()

      assert result == [5, 3, 1, 4, 2]
    end
  end
end
