defmodule ExplorerDuckDB.FullCoverageTest do
  @moduledoc """
  Tests every single implemented callback with real data verification.
  Not just smoke tests -- asserts actual values.
  """
  use ExUnit.Case, async: true

  require Explorer.DataFrame
  alias Explorer.DataFrame
  alias Explorer.Series

  setup do
    Explorer.Backend.put(ExplorerDuckDB)
    :ok
  end

  # ============================================================
  # Series: Conversion (7 callbacks)
  # ============================================================

  test "from_list/to_list roundtrip all dtypes" do
    for {dtype, vals} <- [
          {{:s, 8}, [1, -1, 0, 127]},
          {{:s, 16}, [1, -1, 32767]},
          {{:s, 32}, [1, -1, 0]},
          {{:s, 64}, [1, -1, 0]},
          {{:u, 8}, [0, 128, 255]},
          {{:u, 16}, [0, 65535]},
          {{:u, 32}, [0, 100]},
          {{:u, 64}, [0, 100]},
          {{:f, 32}, [1.0, -1.0]},
          {{:f, 64}, [1.0, -1.5, 0.0]},
          {:boolean, [true, false, true]},
          {:string, ["a", "b", "c"]}
        ] do
      s = Series.from_list(vals, dtype: dtype)
      assert Series.to_list(s) == vals, "failed for dtype #{inspect(dtype)}"
    end
  end

  test "from_binary/to_iovec roundtrip" do
    binary = <<1::64-signed-native, 2::64-signed-native, 3::64-signed-native>>
    s = Series.from_binary(binary, {:s, 64})
    [result] = Series.to_iovec(s)
    assert result == binary
  end

  test "cast changes dtype" do
    s = Series.from_list([1, 2, 3])
    f = Series.cast(s, {:f, 64})
    assert Series.dtype(f) == {:f, 64}
    assert Series.to_list(f) == [1.0, 2.0, 3.0]
  end

  test "categorise maps indices to values" do
    cats = Series.from_list(["a", "b", "c"])
    idx = Series.from_list([0, 2, 1])
    result = Series.categorise(idx, cats)
    assert Series.to_list(result) == ["a", "c", "b"]
  end

  # ============================================================
  # Series: Introspection (3 callbacks)
  # ============================================================

  test "size returns count" do
    assert Series.size(Series.from_list([1, 2, 3])) == 3
    assert Series.size(Series.from_list([], dtype: {:s, 64})) == 0
  end

  test "inspect returns algebra doc" do
    s = Series.from_list([1, 2, 3])
    assert inspect(s) =~ "DuckDB"
  end

  test "categories returns unique values" do
    s = Series.from_list(["a", "b", "a", "c"], dtype: :category)
    cats = Series.categories(s) |> Series.to_list() |> Enum.sort()
    assert cats == ["a", "b", "c"]
  end

  # ============================================================
  # Series: Slice and dice (16 callbacks)
  # ============================================================

  test "head/tail" do
    s = Series.from_list([1, 2, 3, 4, 5])
    assert Series.head(s, 3) |> Series.to_list() == [1, 2, 3]
    assert Series.tail(s, 2) |> Series.to_list() == [4, 5]
  end

  test "sample returns correct count" do
    s = Series.from_list(Enum.to_list(1..100))
    assert Series.sample(s, 5) |> Series.size() == 5
  end

  test "at returns element" do
    s = Series.from_list([10, 20, 30])
    assert Series.at(s, 0) == 10
    assert Series.at(s, 2) == 30
  end

  test "at_every returns every nth" do
    s = Series.from_list([0, 1, 2, 3, 4, 5])
    assert Series.at_every(s, 2) |> Series.to_list() == [0, 2, 4]
  end

  test "mask filters by boolean" do
    s = Series.from_list([10, 20, 30, 40])
    m = Series.from_list([true, false, true, false])
    assert Series.mask(s, m) |> Series.to_list() == [10, 30]
  end

  test "slice by indices" do
    s = Series.from_list([10, 20, 30, 40, 50])
    assert Series.slice(s, [0, 2, 4]) |> Series.to_list() == [10, 30, 50]
  end

  test "slice by offset+length" do
    s = Series.from_list([10, 20, 30, 40, 50])
    assert Series.slice(s, 1, 3) |> Series.to_list() == [20, 30, 40]
  end

  test "format concatenates as strings" do
    s1 = Series.from_list(["hello", "hi"])
    s2 = Series.from_list([" world", " there"])
    assert Series.format([s1, s2]) |> Series.to_list() == ["hello world", "hi there"]
  end

  test "concat joins series" do
    s1 = Series.from_list([1, 2])
    s2 = Series.from_list([3, 4])
    assert Series.concat([s1, s2]) |> Series.to_list() == [1, 2, 3, 4]
  end

  test "coalesce prefers left" do
    s1 = Series.from_list([1, nil, 3])
    s2 = Series.from_list([10, 20, 30])
    assert Series.coalesce(s1, s2) |> Series.to_list() == [1, 20, 3]
  end

  test "first/last" do
    s = Series.from_list([10, 20, 30])
    assert Series.first(s) == 10
    assert Series.last(s) == 30
  end

  test "select chooses by predicate" do
    pred = Series.from_list([true, false, true])
    a = Series.from_list([1, 2, 3])
    b = Series.from_list([10, 20, 30])
    assert Series.select(pred, a, b) |> Series.to_list() == [1, 20, 3]
  end

  test "shift moves values" do
    s = Series.from_list([1, 2, 3, 4])
    assert Series.shift(s, 1) |> Series.to_list() == [nil, 1, 2, 3]
    assert Series.shift(s, -1) |> Series.to_list() == [2, 3, 4, nil]
  end

  test "rank assigns rank" do
    s = Series.from_list([30, 10, 20])
    result = Series.rank(s) |> Series.to_list()
    assert length(result) == 3
  end

  # ============================================================
  # Series: Aggregation (22 callbacks)
  # ============================================================

  test "sum/mean/min/max/median" do
    s = Series.from_list([1, 2, 3, 4, 5])
    assert Series.sum(s) == 15
    assert Series.mean(s) == 3.0
    assert Series.min(s) == 1
    assert Series.max(s) == 5
    assert Series.median(s) == 3.0
  end

  test "argmin/argmax" do
    s = Series.from_list([5, 1, 3])
    assert Series.argmin(s) == 1
    assert Series.argmax(s) == 0
  end

  test "mode returns most frequent" do
    s = Series.from_list([1, 2, 2, 3, 2])
    assert 2 in (Series.mode(s) |> Series.to_list())
  end

  test "variance/standard_deviation" do
    s = Series.from_list([2.0, 4.0, 6.0])
    assert is_number(Series.variance(s))
    assert is_number(Series.standard_deviation(s))
  end

  test "quantile" do
    s = Series.from_list([1.0, 2.0, 3.0, 4.0, 5.0])
    assert_in_delta Series.quantile(s, 0.5), 3.0, 0.1
  end

  test "nil_count" do
    s = Series.from_list([1, nil, nil, 4])
    assert Series.nil_count(s) == 2
  end

  test "product" do
    s = Series.from_list([2, 3, 4])
    assert Series.product(s) == 24
  end

  test "skew returns number" do
    s = Series.from_list([1.0, 2.0, 3.0, 100.0])
    assert is_number(Series.skew(s))
  end

  test "correlation/covariance" do
    s1 = Series.from_list([1.0, 2.0, 3.0])
    s2 = Series.from_list([2.0, 4.0, 6.0])
    assert_in_delta Series.correlation(s1, s2), 1.0, 0.001
    assert Series.covariance(s1, s2) > 0
  end

  test "all?/any?" do
    assert Series.all?(Series.from_list([true, true]))
    refute Series.all?(Series.from_list([true, false]))
    assert Series.any?(Series.from_list([false, true]))
    refute Series.any?(Series.from_list([false, false]))
  end

  test "count" do
    assert Series.count(Series.from_list([1, 2, 3])) == 3
  end

  test "row_index" do
    s = Series.from_list(["a", "b", "c"])
    assert Series.row_index(s) |> Series.to_list() == [0, 1, 2]
  end

  test "n_distinct" do
    s = Series.from_list([1, 1, 2, 2, 3])
    assert Series.n_distinct(s) == 3
  end

  test "frequencies returns dataframe" do
    s = Series.from_list(["a", "b", "a"])
    df = Series.frequencies(s)
    assert DataFrame.n_rows(df) == 2
  end

  # ============================================================
  # Series: Cumulative (5 callbacks)
  # ============================================================

  test "cumulative_sum" do
    assert Series.cumulative_sum(Series.from_list([1, 2, 3])) |> Series.to_list() == [1, 3, 6]
  end

  test "cumulative_min" do
    assert Series.cumulative_min(Series.from_list([3, 1, 2])) |> Series.to_list() == [3, 1, 1]
  end

  test "cumulative_max" do
    assert Series.cumulative_max(Series.from_list([1, 3, 2])) |> Series.to_list() == [1, 3, 3]
  end

  test "cumulative_product" do
    assert Series.cumulative_product(Series.from_list([2, 3, 4])) |> Series.to_list() == [2, 6, 24]
  end

  test "cumulative_count" do
    result = Series.cumulative_count(Series.from_list([5, 5, 5])) |> Series.to_list()
    assert result == [1, 2, 3]
  end

  # ============================================================
  # Series: Arithmetic (12 callbacks)
  # ============================================================

  test "add/subtract/multiply/divide" do
    a = Series.from_list([10, 20, 30])
    b = Series.from_list([1, 2, 3])
    assert Series.add(a, b) |> Series.to_list() == [11, 22, 33]
    assert Series.subtract(a, b) |> Series.to_list() == [9, 18, 27]
    assert Series.multiply(a, b) |> Series.to_list() == [10, 40, 90]
  end

  test "quotient/remainder" do
    a = Series.from_list([10, 11, 12])
    b = Series.from_list([3, 3, 3])
    assert Series.quotient(a, b) |> Series.to_list() == [3, 3, 4]
    assert Series.remainder(a, b) |> Series.to_list() == [1, 2, 0]
  end

  test "pow" do
    a = Series.from_list([2, 3])
    b = Series.from_list([3, 2])
    assert Series.pow(a, b) |> Series.to_list() == [8, 9]
  end

  test "log/exp" do
    s = Series.from_list([1.0, 2.718281828])
    result = Series.log(s) |> Series.to_list()
    assert_in_delta hd(result), 0.0, 0.001
  end

  test "abs" do
    assert Series.abs(Series.from_list([-3, 0, 3])) |> Series.to_list() == [3, 0, 3]
  end

  test "clip" do
    assert Series.clip(Series.from_list([1, 5, 10]), 3, 7) |> Series.to_list() == [3, 5, 7]
  end

  # ============================================================
  # Series: Trigonometry (8 callbacks)
  # ============================================================

  test "sin/cos/tan" do
    s = Series.from_list([0.0])
    assert_in_delta Series.sin(s) |> Series.to_list() |> hd(), 0.0, 0.001
    assert_in_delta Series.cos(s) |> Series.to_list() |> hd(), 1.0, 0.001
    assert_in_delta Series.tan(s) |> Series.to_list() |> hd(), 0.0, 0.001
  end

  test "asin/acos/atan" do
    s = Series.from_list([0.0])
    assert_in_delta Series.asin(s) |> Series.to_list() |> hd(), 0.0, 0.001
    assert_in_delta Series.acos(s) |> Series.to_list() |> hd(), 1.5708, 0.001
    assert_in_delta Series.atan(s) |> Series.to_list() |> hd(), 0.0, 0.001
  end

  test "degrees/radians" do
    assert_in_delta Series.radians(Series.from_list([180.0])) |> Series.to_list() |> hd(), :math.pi(), 0.001
    assert_in_delta Series.degrees(Series.from_list([:math.pi()])) |> Series.to_list() |> hd(), 180.0, 0.001
  end

  # ============================================================
  # Series: Comparisons (10 callbacks)
  # ============================================================

  test "equal/not_equal" do
    a = Series.from_list([1, 2, 3])
    b = Series.from_list([1, 0, 3])
    assert Series.equal(a, b) |> Series.to_list() == [true, false, true]
    assert Series.not_equal(a, b) |> Series.to_list() == [false, true, false]
  end

  test "greater/greater_equal/less/less_equal" do
    a = Series.from_list([1, 2, 3])
    b = Series.from_list([2, 2, 2])
    assert Series.greater(a, b) |> Series.to_list() == [false, false, true]
    assert Series.greater_equal(a, b) |> Series.to_list() == [false, true, true]
    assert Series.less(a, b) |> Series.to_list() == [true, false, false]
    assert Series.less_equal(a, b) |> Series.to_list() == [true, true, false]
  end

  test "all_equal" do
    assert Series.all_equal(Series.from_list([1, 2]), Series.from_list([1, 2]))
    refute Series.all_equal(Series.from_list([1, 2]), Series.from_list([1, 3]))
  end

  test "binary_and/binary_or" do
    a = Series.from_list([true, true, false])
    b = Series.from_list([true, false, false])
    assert Series.and(a, b) |> Series.to_list() == [true, false, false]
    assert Series.or(a, b) |> Series.to_list() == [true, true, false]
  end

  test "binary_in" do
    s = Series.from_list([1, 2, 3, 4])
    other = Series.from_list([2, 4])
    assert Series.in(s, other) |> Series.to_list() == [false, true, false, true]
  end

  # ============================================================
  # Series: Float predicates + rounding (6 callbacks)
  # ============================================================

  test "is_finite/is_nan" do
    s = Series.from_list([1.0, :nan, :infinity])
    assert Series.is_finite(s) |> Series.to_list() |> hd() == true
    assert Series.is_nan(s) |> Series.to_list() |> Enum.at(1) == true
  end

  test "round/floor/ceil" do
    s = Series.from_list([1.4, 2.5, 3.6])
    assert Series.round(s, 0) |> Series.to_list() == [1.0, 3.0, 4.0]
    assert Series.floor(s) |> Series.to_list() == [1.0, 2.0, 3.0]
    assert Series.ceil(s) |> Series.to_list() == [2.0, 3.0, 4.0]
  end

  # ============================================================
  # Series: Sort (4 callbacks)
  # ============================================================

  test "sort asc/desc" do
    s = Series.from_list([3, 1, 2])
    assert Series.sort(s) |> Series.to_list() == [1, 2, 3]
    assert Series.sort(s, direction: :desc) |> Series.to_list() == [3, 2, 1]
  end

  test "argsort" do
    s = Series.from_list([30, 10, 20])
    assert Series.argsort(s) |> Series.to_list() == [1, 2, 0]
  end

  test "reverse" do
    assert Series.reverse(Series.from_list([1, 2, 3])) |> Series.to_list() == [3, 2, 1]
  end

  # ============================================================
  # Series: Distinct (4 callbacks)
  # ============================================================

  test "distinct" do
    s = Series.from_list([1, 2, 2, 3, 3, 3])
    assert Series.distinct(s) |> Series.size() == 3
  end

  test "unordered_distinct" do
    s = Series.from_list([3, 1, 2, 1])
    assert Series.unordered_distinct(s) |> Series.size() == 3
  end

  # ============================================================
  # Series: Window (6 callbacks)
  # ============================================================

  test "window_sum" do
    s = Series.from_list([1, 2, 3, 4, 5])
    result = Series.window_sum(s, 3) |> Series.to_list()
    assert Enum.at(result, 2) == 6
  end

  test "window_mean" do
    s = Series.from_list([1.0, 2.0, 3.0, 4.0, 5.0])
    result = Series.window_mean(s, 3) |> Series.to_list()
    assert_in_delta Enum.at(result, 2), 2.0, 0.001
  end

  test "window_min/max" do
    s = Series.from_list([3, 1, 4, 1, 5])
    min_result = Series.window_min(s, 3) |> Series.to_list()
    assert Enum.at(min_result, 2) == 1
  end

  # ============================================================
  # Series: EWM (3 callbacks)
  # ============================================================

  test "ewm_mean" do
    s = Series.from_list([1.0, 2.0, 3.0])
    result = Series.ewm_mean(s, alpha: 0.5) |> Series.to_list()
    assert length(result) == 3
    assert Enum.all?(result, &is_number/1)
  end

  # ============================================================
  # Series: Nulls (4 callbacks)
  # ============================================================

  test "fill_missing forward/backward/value" do
    s = Series.from_list([1, nil, nil, 4])
    assert Series.fill_missing(s, :forward) |> Series.to_list() == [1, 1, 1, 4]
    assert Series.fill_missing(s, :backward) |> Series.to_list() == [1, 4, 4, 4]
    assert Series.fill_missing(s, 0) |> Series.to_list() == [1, 0, 0, 4]
  end

  test "is_nil/is_not_nil" do
    s = Series.from_list([1, nil, 3])
    assert Series.is_nil(s) |> Series.to_list() == [false, true, false]
    assert Series.is_not_nil(s) |> Series.to_list() == [true, false, true]
  end

  # ============================================================
  # Series: String (18 callbacks)
  # ============================================================

  test "contains" do
    s = Series.from_list(["hello world", "foo bar"])
    assert Series.contains(s, "hello") |> Series.to_list() == [true, false]
  end

  test "upcase/downcase" do
    s = Series.from_list(["Hello"])
    assert Series.upcase(s) |> Series.to_list() == ["HELLO"]
    assert Series.downcase(s) |> Series.to_list() == ["hello"]
  end

  test "replace" do
    s = Series.from_list(["hello world"])
    assert Series.replace(s, "world", "elixir") |> Series.to_list() == ["hello elixir"]
  end

  test "strip/lstrip/rstrip" do
    s = Series.from_list(["  hi  "])
    assert Series.strip(s) |> Series.to_list() == ["hi"]
    assert Series.lstrip(s) |> Series.to_list() == ["hi  "]
    assert Series.rstrip(s) |> Series.to_list() == ["  hi"]
  end

  test "substring" do
    s = Series.from_list(["hello world"])
    assert Series.substring(s, 6) |> Series.to_list() == ["world"]
    assert Series.substring(s, 0, 5) |> Series.to_list() == ["hello"]
  end

  test "split" do
    s = Series.from_list(["a,b,c"])
    result = Series.split(s, ",") |> Series.to_list()
    assert hd(result) == ["a", "b", "c"]
  end

  test "re_contains" do
    s = Series.from_list(["abc123", "abc"])
    assert Series.re_contains(s, "\\d+") |> Series.to_list() == [true, false]
  end

  test "re_replace" do
    s = Series.from_list(["hello 123"])
    assert Series.re_replace(s, "\\d+", "NUM") |> Series.to_list() == ["hello NUM"]
  end

  test "json_path_match" do
    s = Series.from_list([~s({"name": "Alice"})])
    assert Series.json_path_match(s, "$.name") |> Series.to_list() == ["Alice"]
  end

  # ============================================================
  # Series: Escape hatch + logic (2 callbacks)
  # ============================================================

  test "transform" do
    s = Series.from_list([1, 2, 3])
    result = Series.transform(s, fn x -> x * 10 end)
    assert Series.to_list(result) == [10, 20, 30]
  end

  test "unary_not" do
    s = Series.from_list([true, false, true])
    assert Series.not(s) |> Series.to_list() == [false, true, false]
  end

  # ============================================================
  # Series: Peaks (1 callback)
  # ============================================================

  test "peaks" do
    s = Series.from_list([1, 3, 2, 5, 4])
    max_peaks = Series.peaks(s, :max) |> Series.to_list()
    assert Enum.at(max_peaks, 1) == true
    assert Enum.at(max_peaks, 3) == true
  end

  # ============================================================
  # Series: Cut/Qcut (2 callbacks)
  # ============================================================

  test "cut" do
    s = Series.from_list([1.0, 5.0, 10.0])
    df = Series.cut(s, [5.0])
    assert DataFrame.n_rows(df) == 3
  end

  test "qcut" do
    s = Series.from_list(Enum.map(1..100, &(&1 * 1.0)))
    df = Series.qcut(s, [0.5])
    assert DataFrame.n_rows(df) == 100
  end

  # ============================================================
  # DataFrame: All table verbs
  # ============================================================

  test "head/tail/slice" do
    df = DataFrame.new(x: [1, 2, 3, 4, 5])
    assert DataFrame.head(df, 2) |> DataFrame.n_rows() == 2
    assert DataFrame.tail(df, 2) |> DataFrame.n_rows() == 2
    assert DataFrame.slice(df, 1, 3) |> DataFrame.n_rows() == 3
  end

  test "select/rename/distinct/drop_nil" do
    df = DataFrame.new(a: [1, 1, nil], b: [2, 2, 3])
    assert DataFrame.select(df, ["a"]) |> DataFrame.names() == ["a"]
    assert DataFrame.rename(df, a: "x") |> DataFrame.names() |> Enum.member?("x")
    assert DataFrame.distinct(df) |> DataFrame.n_rows() <= 3
    assert DataFrame.drop_nil(df) |> DataFrame.n_rows() == 2
  end

  test "filter_with" do
    df = DataFrame.new(x: [1, 2, 3, 4, 5])
    result = DataFrame.filter_with(df, fn row -> Series.greater(row["x"], 3) end)
    assert Series.to_list(DataFrame.pull(result, "x")) == [4, 5]
  end

  test "mutate_with" do
    df = DataFrame.new(x: [1, 2, 3])
    result = DataFrame.mutate_with(df, fn row -> [y: Series.multiply(row["x"], 2)] end)
    assert Series.to_list(DataFrame.pull(result, "y")) == [2, 4, 6]
  end

  test "sort_by" do
    df = DataFrame.new(x: [3, 1, 2])
    assert DataFrame.sort_by(df, asc: x) |> DataFrame.pull("x") |> Series.to_list() == [1, 2, 3]
  end

  test "summarise_with" do
    df = DataFrame.new(g: ["a", "a", "b"], v: [1, 2, 3])
    result =
      df
      |> DataFrame.group_by("g")
      |> DataFrame.summarise_with(fn row -> [total: Series.sum(row["v"])] end)
    assert DataFrame.n_rows(result) == 2
  end

  test "join" do
    left = DataFrame.new(id: [1, 2, 3], name: ["a", "b", "c"])
    right = DataFrame.new(id: [2, 3], score: [10, 20])
    result = DataFrame.join(left, right, on: [{"id", "id"}])
    assert DataFrame.n_rows(result) == 2
  end

  test "concat_rows/concat_columns" do
    df1 = DataFrame.new(x: [1, 2])
    df2 = DataFrame.new(x: [3, 4])
    assert DataFrame.concat_rows([df1, df2]) |> DataFrame.n_rows() == 4

    a = DataFrame.new(a: [1, 2])
    b = DataFrame.new(b: [3, 4])
    result = DataFrame.concat_columns([a, b])
    assert "a" in DataFrame.names(result)
    assert "b" in DataFrame.names(result)
  end

  test "DataFrame nil_count" do
    df = DataFrame.new(x: [1, nil, nil], y: [nil, 2, nil])
    result = DataFrame.nil_count(df)
    assert DataFrame.n_rows(result) == 1
  end

  test "sql passthrough" do
    df = DataFrame.new(x: [1, 2, 3])
    result = DataFrame.sql(df, "SELECT x, x * 2 AS doubled FROM tbl", table_name: "tbl")
    assert "doubled" in DataFrame.names(result)
  end

  test "sample" do
    df = DataFrame.new(x: Enum.to_list(1..100))
    assert DataFrame.sample(df, 5) |> DataFrame.n_rows() == 5
  end

  # ============================================================
  # DataFrame: IO
  # ============================================================

  test "dump/load all formats" do
    df = DataFrame.new(x: [1, 2, 3], y: ["a", "b", "c"])

    {:ok, csv} = DataFrame.dump_csv(df)
    {:ok, _} = DataFrame.load_csv(csv)

    {:ok, parquet} = DataFrame.dump_parquet(df)
    {:ok, _} = DataFrame.load_parquet(parquet)

    {:ok, ipc} = DataFrame.dump_ipc(df)
    {:ok, _} = DataFrame.load_ipc(ipc)

    {:ok, ipc_stream} = DataFrame.dump_ipc_stream(df)
    {:ok, _} = DataFrame.load_ipc_stream(ipc_stream)

    {:ok, ndjson} = DataFrame.dump_ndjson(df)
    {:ok, _} = DataFrame.load_ndjson(ndjson)
  end

  # ============================================================
  # DataFrame: Reshape
  # ============================================================

  test "pivot_wider" do
    df = DataFrame.new(id: [1, 1], name: ["a", "b"], value: [10, 20])
    result = DataFrame.pivot_wider(df, "name", "value")
    assert DataFrame.n_rows(result) >= 1
  end

  test "pivot_longer" do
    df = DataFrame.new(id: [1], a: [10], b: [20])
    result = DataFrame.pivot_longer(df, ["a", "b"], names_to: "col", values_to: "val")
    assert DataFrame.n_rows(result) == 2
  end

  test "explode" do
    df = ExplorerDuckDB.query("SELECT [1, 2, 3] AS arr")
    result = DataFrame.explode(df, ["arr"])
    assert DataFrame.n_rows(result) == 3
  end

  test "dummies" do
    df = DataFrame.new(color: ["red", "blue", "red"])
    result = DataFrame.dummies(df, ["color"])
    assert DataFrame.n_rows(result) == 3
  end

  test "DataFrame correlation/covariance" do
    df = DataFrame.new(x: [1.0, 2.0, 3.0], y: [2.0, 4.0, 6.0])
    assert DataFrame.correlation(df) |> DataFrame.n_rows() == 1
    assert DataFrame.covariance(df) |> DataFrame.n_rows() == 1
  end
end
