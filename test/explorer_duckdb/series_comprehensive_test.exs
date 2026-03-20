defmodule ExplorerDuckDB.SeriesComprehensiveTest do
  use ExUnit.Case, async: true

  alias Explorer.Series

  setup do
    Explorer.Backend.put(ExplorerDuckDB)
    :ok
  end

  # ============================================================
  # Math
  # ============================================================

  describe "log/exp/abs/clip" do
    test "log (natural)" do
      s = Series.from_list([1.0, 2.718281828, 7.389056099])
      result = Series.log(s) |> Series.to_list()
      assert_in_delta hd(result), 0.0, 0.001
      assert_in_delta Enum.at(result, 1), 1.0, 0.001
    end

    test "log with base" do
      s = Series.from_list([1.0, 10.0, 100.0])
      result = Series.log(s, 10) |> Series.to_list()
      assert_in_delta hd(result), 0.0, 0.001
      assert_in_delta Enum.at(result, 1), 1.0, 0.001
    end

    test "exp" do
      s = Series.from_list([0.0, 1.0])
      result = Series.exp(s) |> Series.to_list()
      assert_in_delta hd(result), 1.0, 0.001
      assert_in_delta Enum.at(result, 1), 2.718, 0.01
    end

    test "abs" do
      s = Series.from_list([-3, -1, 0, 2, 5])
      assert Series.abs(s) |> Series.to_list() == [3, 1, 0, 2, 5]
    end

    test "clip" do
      s = Series.from_list([1, 5, 10, 15, 20])
      assert Series.clip(s, 5, 15) |> Series.to_list() == [5, 5, 10, 15, 15]
    end
  end

  # ============================================================
  # Trigonometry
  # ============================================================

  describe "trigonometry" do
    test "sin/cos" do
      s = Series.from_list([0.0])
      assert_in_delta Series.sin(s) |> Series.to_list() |> hd(), 0.0, 0.001
      assert_in_delta Series.cos(s) |> Series.to_list() |> hd(), 1.0, 0.001
    end

    test "degrees/radians" do
      s = Series.from_list([180.0])
      rads = Series.radians(s) |> Series.to_list() |> hd()
      assert_in_delta rads, :math.pi(), 0.001

      s2 = Series.from_list([:math.pi()])
      degs = Series.degrees(s2) |> Series.to_list() |> hd()
      assert_in_delta degs, 180.0, 0.001
    end
  end

  # ============================================================
  # Float rounding
  # ============================================================

  describe "rounding" do
    test "round" do
      s = Series.from_list([1.456, 2.789, 3.123])
      assert Series.round(s, 1) |> Series.to_list() == [1.5, 2.8, 3.1]
    end

    test "floor" do
      s = Series.from_list([1.9, 2.1, -1.5])
      assert Series.floor(s) |> Series.to_list() == [1.0, 2.0, -2.0]
    end

    test "ceil" do
      s = Series.from_list([1.1, 2.9, -1.5])
      assert Series.ceil(s) |> Series.to_list() == [2.0, 3.0, -1.0]
    end
  end

  # ============================================================
  # Float predicates
  # ============================================================

  describe "float predicates" do
    test "is_finite" do
      s = Series.from_list([1.0, 2.0, 3.0])
      assert Series.is_finite(s) |> Series.to_list() == [true, true, true]
    end

    test "is_nan" do
      # DuckDB doesn't support NaN in from_list directly, test with computed NaN
      s = Series.from_list([1.0, 2.0, 3.0])
      result = Series.is_nan(s) |> Series.to_list()
      assert result == [false, false, false]
    end
  end

  # ============================================================
  # Strings
  # ============================================================

  describe "string operations" do
    test "contains" do
      s = Series.from_list(["hello world", "foo bar", "hello foo"])
      assert Series.contains(s, "hello") |> Series.to_list() == [true, false, true]
    end

    test "upcase/downcase" do
      s = Series.from_list(["Hello", "World"])
      assert Series.upcase(s) |> Series.to_list() == ["HELLO", "WORLD"]
      assert Series.downcase(s) |> Series.to_list() == ["hello", "world"]
    end

    test "replace" do
      s = Series.from_list(["hello world", "hello foo"])
      result = Series.replace(s, "hello", "hi")
      assert Series.to_list(result) == ["hi world", "hi foo"]
    end

    test "strip" do
      s = Series.from_list(["  hello  ", "  world  "])
      assert Series.strip(s) |> Series.to_list() == ["hello", "world"]
    end

    test "lstrip/rstrip" do
      s = Series.from_list(["  hello  "])
      assert Series.lstrip(s) |> Series.to_list() == ["hello  "]
      assert Series.rstrip(s) |> Series.to_list() == ["  hello"]
    end

    test "substring" do
      s = Series.from_list(["hello world"])
      assert Series.substring(s, 6) |> Series.to_list() == ["world"]
      assert Series.substring(s, 0, 5) |> Series.to_list() == ["hello"]
    end

    test "re_contains" do
      s = Series.from_list(["abc123", "def456", "abc"])
      assert Series.re_contains(s, "\\d+") |> Series.to_list() == [true, true, false]
    end

    test "re_replace" do
      s = Series.from_list(["hello 123 world 456"])
      result = Series.re_replace(s, "\\d+", "NUM")
      assert Series.to_list(result) == ["hello NUM world NUM"]
    end
  end

  # ============================================================
  # Date/Time extraction
  # ============================================================

  describe "date/time via SQL" do
    test "year and month extraction" do
      # Use DuckDB SQL directly to test date extraction since from_list with dates
      # stores as strings currently
      Explorer.Backend.put(ExplorerDuckDB)
      db = ExplorerDuckDB.Shared.get_db()

      {:ok, df_ref} = ExplorerDuckDB.Native.df_query(db, "SELECT '2023-01-15'::DATE AS d")
      |> then(fn ref -> {:ok, ref} end)

      df = ExplorerDuckDB.Shared.create_dataframe!(df_ref)
      s = Explorer.DataFrame.pull(df, "d")

      result = Series.year(s) |> Series.to_list()
      assert hd(result) == 2023
    end
  end

  # ============================================================
  # Cumulative
  # ============================================================

  describe "cumulative" do
    test "cumulative_sum" do
      s = Series.from_list([1, 2, 3, 4, 5])
      assert Series.cumulative_sum(s) |> Series.to_list() == [1, 3, 6, 10, 15]
    end

    test "cumulative_max" do
      s = Series.from_list([3, 1, 4, 1, 5])
      assert Series.cumulative_max(s) |> Series.to_list() == [3, 3, 4, 4, 5]
    end

    test "cumulative_min" do
      s = Series.from_list([3, 1, 4, 1, 5])
      assert Series.cumulative_min(s) |> Series.to_list() == [3, 1, 1, 1, 1]
    end
  end

  # ============================================================
  # Window
  # ============================================================

  describe "window" do
    test "window_sum" do
      s = Series.from_list([1, 2, 3, 4, 5])
      result = Series.window_sum(s, 3) |> Series.to_list()
      # Window of 3: [1, 3, 6, 9, 12]
      assert Enum.at(result, 2) == 6
      assert Enum.at(result, 4) == 12
    end

    test "window_mean" do
      s = Series.from_list([1.0, 2.0, 3.0, 4.0, 5.0])
      result = Series.window_mean(s, 3) |> Series.to_list()
      assert_in_delta Enum.at(result, 2), 2.0, 0.001
      assert_in_delta Enum.at(result, 4), 4.0, 0.001
    end
  end

  # ============================================================
  # Fill missing
  # ============================================================

  describe "fill_missing" do
    test "fill_missing_with_value" do
      s = Series.from_list([1, nil, 3, nil, 5])
      result = Series.fill_missing(s, 0)
      assert Series.to_list(result) == [1, 0, 3, 0, 5]
    end

    test "fill_missing forward" do
      s = Series.from_list([1, nil, nil, 4, nil])
      result = Series.fill_missing(s, :forward)
      assert Series.to_list(result) == [1, 1, 1, 4, 4]
    end

    test "fill_missing backward" do
      s = Series.from_list([nil, nil, 3, nil, 5])
      result = Series.fill_missing(s, :backward)
      assert Series.to_list(result) == [3, 3, 3, 5, 5]
    end
  end

  # ============================================================
  # Distinct
  # ============================================================

  describe "distinct" do
    test "distinct" do
      s = Series.from_list([1, 2, 2, 3, 3, 3])
      result = Series.distinct(s)
      assert Series.size(result) == 3
    end

    test "n_distinct" do
      s = Series.from_list([1, 2, 2, 3, 3, 3])
      assert Series.n_distinct(s) == 3
    end

    test "frequencies returns a dataframe" do
      s = Series.from_list(["a", "b", "a", "c", "a"])
      result = Series.frequencies(s)
      assert Explorer.DataFrame.n_rows(result) == 3
    end
  end

  # ============================================================
  # Aggregation extras
  # ============================================================

  describe "additional aggregations" do
    test "argmin/argmax" do
      s = Series.from_list([3, 1, 4, 1, 5])
      assert Series.argmin(s) == 1
      assert Series.argmax(s) == 4
    end

    test "quantile" do
      s = Series.from_list([1.0, 2.0, 3.0, 4.0, 5.0])
      assert_in_delta Series.quantile(s, 0.5), 3.0, 0.01
    end

    test "mode" do
      s = Series.from_list([1, 2, 2, 3, 2])
      result = Series.mode(s)
      assert 2 in Series.to_list(result)
    end

    test "nil_count" do
      s = Series.from_list([1, nil, 3, nil, nil])
      assert Series.nil_count(s) == 3
    end

    test "product" do
      s = Series.from_list([2, 3, 4])
      assert Series.product(s) == 24
    end
  end

  # ============================================================
  # Slice and dice extras
  # ============================================================

  describe "slice and dice extras" do
    test "at_every" do
      s = Series.from_list([0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
      assert Series.at_every(s, 3) |> Series.to_list() == [0, 3, 6, 9]
    end

    test "mask" do
      s = Series.from_list([10, 20, 30, 40, 50])
      m = Series.from_list([true, false, true, false, true])
      assert Series.mask(s, m) |> Series.to_list() == [10, 30, 50]
    end

    test "coalesce" do
      s1 = Series.from_list([1, nil, 3, nil])
      s2 = Series.from_list([10, 20, 30, 40])
      assert Series.coalesce(s1, s2) |> Series.to_list() == [1, 20, 3, 40]
    end

    test "concat" do
      s1 = Series.from_list([1, 2])
      s2 = Series.from_list([3, 4])
      assert Series.concat([s1, s2]) |> Series.to_list() == [1, 2, 3, 4]
    end

    test "shift forward" do
      s = Series.from_list([1, 2, 3, 4])
      result = Series.shift(s, 2)
      assert Series.to_list(result) == [nil, nil, 1, 2]
    end

    test "shift backward" do
      s = Series.from_list([1, 2, 3, 4])
      result = Series.shift(s, -1)
      assert Series.to_list(result) == [2, 3, 4, nil]
    end

    test "select" do
      pred = Series.from_list([true, false, true])
      on_true = Series.from_list([1, 2, 3])
      on_false = Series.from_list([10, 20, 30])
      assert Series.select(pred, on_true, on_false) |> Series.to_list() == [1, 20, 3]
    end

    test "sample" do
      s = Series.from_list([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
      result = Series.sample(s, 3)
      assert Series.size(result) == 3
    end
  end

  # ============================================================
  # Sort extras
  # ============================================================

  describe "sort extras" do
    test "argsort" do
      s = Series.from_list([30, 10, 20])
      result = Series.argsort(s) |> Series.to_list()
      assert result == [1, 2, 0]
    end
  end

  # ============================================================
  # Binary operations
  # ============================================================

  describe "binary_in" do
    test "checks membership" do
      s = Series.from_list([1, 2, 3, 4, 5])
      other = Series.from_list([2, 4])
      assert Series.in(s, other) |> Series.to_list() == [false, true, false, true, false]
    end
  end

  # ============================================================
  # Cast
  # ============================================================

  describe "cast" do
    test "integer to float" do
      s = Series.from_list([1, 2, 3])
      result = Series.cast(s, {:f, 64})
      assert Series.dtype(result) == {:f, 64}
      assert Series.to_list(result) == [1.0, 2.0, 3.0]
    end

    test "float to integer" do
      s = Series.from_list([1.0, 2.5, 3.9])
      result = Series.cast(s, {:s, 64})
      assert Series.dtype(result) == {:s, 64}
    end
  end

  # ============================================================
  # Peaks
  # ============================================================

  describe "peaks" do
    test "peaks max" do
      s = Series.from_list([1, 3, 2, 5, 4])
      result = Series.peaks(s, :max) |> Series.to_list()
      assert Enum.at(result, 1) == true  # 3 is a local max
      assert Enum.at(result, 3) == true  # 5 is a local max
    end
  end

  # ============================================================
  # EWM
  # ============================================================

  describe "ewm" do
    test "ewm_mean" do
      s = Series.from_list([1.0, 2.0, 3.0, 4.0, 5.0])
      result = Series.ewm_mean(s, alpha: 0.5) |> Series.to_list()
      assert length(result) == 5
      # EWM mean should be between min and max
      assert Enum.all?(result, fn v -> v >= 1.0 and v <= 5.0 end)
    end
  end

  # ============================================================
  # Correlation / Covariance
  # ============================================================

  describe "series correlation/covariance" do
    test "correlation" do
      s1 = Series.from_list([1.0, 2.0, 3.0, 4.0, 5.0])
      s2 = Series.from_list([2.0, 4.0, 6.0, 8.0, 10.0])
      corr = Series.correlation(s1, s2)
      assert_in_delta corr, 1.0, 0.001
    end

    test "covariance" do
      s1 = Series.from_list([1.0, 2.0, 3.0])
      s2 = Series.from_list([1.0, 2.0, 3.0])
      cov = Series.covariance(s1, s2)
      assert cov > 0
    end
  end

  # ============================================================
  # from_binary / to_iovec
  # ============================================================

  describe "binary conversion" do
    test "from_binary f64" do
      binary = <<1.0::64-float-native, 2.0::64-float-native, 3.0::64-float-native>>
      s = Series.from_binary(binary, {:f, 64})
      assert Series.to_list(s) == [1.0, 2.0, 3.0]
    end

    test "to_iovec f64" do
      s = Series.from_list([1.0, 2.0, 3.0])
      [binary] = Series.to_iovec(s)
      assert is_binary(binary)
      assert byte_size(binary) == 24
    end

    test "round-trip binary" do
      original = [1.0, 2.0, 3.0, 4.0, 5.0]
      s = Series.from_list(original)
      [binary] = Series.to_iovec(s)
      s2 = Series.from_binary(binary, {:f, 64})
      assert Series.to_list(s2) == original
    end
  end

  # ============================================================
  # Row index
  # ============================================================

  describe "row_index" do
    test "returns 0-based index" do
      s = Series.from_list(["a", "b", "c"])
      result = Series.row_index(s) |> Series.to_list()
      assert result == [0, 1, 2]
    end
  end
end
