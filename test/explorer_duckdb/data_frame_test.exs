defmodule ExplorerDuckDB.DataFrameTest do
  use ExUnit.Case, async: true

  require Explorer.DataFrame
  alias Explorer.DataFrame
  alias Explorer.Series

  setup do
    Explorer.Backend.put(ExplorerDuckDB)
    :ok
  end

  describe "from_series/pull" do
    test "creates a dataframe from named series" do
      s1 = Series.from_list([1, 2, 3])
      s2 = Series.from_list(["a", "b", "c"])
      df = DataFrame.new(a: s1, b: s2)

      assert DataFrame.n_rows(df) == 3
      assert DataFrame.names(df) == ["a", "b"]

      pulled = DataFrame.pull(df, "a")
      assert Series.to_list(pulled) == [1, 2, 3]
    end
  end

  describe "head/tail/slice" do
    test "head" do
      df = DataFrame.new(x: Series.from_list([1, 2, 3, 4, 5]))
      result = DataFrame.head(df, 3)
      assert DataFrame.n_rows(result) == 3
      assert Series.to_list(DataFrame.pull(result, "x")) == [1, 2, 3]
    end

    test "tail" do
      df = DataFrame.new(x: Series.from_list([1, 2, 3, 4, 5]))
      result = DataFrame.tail(df, 2)
      assert DataFrame.n_rows(result) == 2
      assert Series.to_list(DataFrame.pull(result, "x")) == [4, 5]
    end

    test "slice with offset and length" do
      df = DataFrame.new(x: Series.from_list([10, 20, 30, 40, 50]))
      result = DataFrame.slice(df, 1, 3)
      assert DataFrame.n_rows(result) == 3
      assert Series.to_list(DataFrame.pull(result, "x")) == [20, 30, 40]
    end
  end

  describe "select" do
    test "selects columns" do
      df = DataFrame.new(a: [1, 2], b: ["x", "y"], c: [1.0, 2.0])
      result = DataFrame.select(df, ["a", "c"])

      assert DataFrame.names(result) == ["a", "c"]
      assert DataFrame.n_rows(result) == 2
    end
  end

  describe "rename" do
    test "renames columns" do
      df = DataFrame.new(a: [1, 2], b: [3, 4])
      result = DataFrame.rename(df, a: "x", b: "y")

      assert "x" in DataFrame.names(result)
      assert "y" in DataFrame.names(result)
      assert Series.to_list(DataFrame.pull(result, "x")) == [1, 2]
      assert Series.to_list(DataFrame.pull(result, "y")) == [3, 4]
    end
  end

  describe "filter_with" do
    test "filters rows with lazy expression" do
      df = DataFrame.new(x: [1, 2, 3, 4, 5], y: ["a", "b", "c", "d", "e"])

      result =
        DataFrame.filter_with(df, fn row ->
          Series.greater(row["x"], 3)
        end)

      assert DataFrame.n_rows(result) == 2
      assert Series.to_list(DataFrame.pull(result, "x")) == [4, 5]
    end
  end

  describe "mutate_with" do
    test "adds a computed column" do
      df = DataFrame.new(x: [1, 2, 3])

      result =
        DataFrame.mutate_with(df, fn row ->
          [doubled: Series.multiply(row["x"], 2)]
        end)

      assert "doubled" in DataFrame.names(result)
      assert Series.to_list(DataFrame.pull(result, "doubled")) == [2, 4, 6]
    end
  end

  describe "sort_with" do
    test "sorts by column ascending" do
      df = DataFrame.new(x: [3, 1, 2])
      result = DataFrame.sort_by(df, asc: x)

      assert Series.to_list(DataFrame.pull(result, "x")) == [1, 2, 3]
    end

    test "sorts by column descending" do
      df = DataFrame.new(x: [3, 1, 2])
      result = DataFrame.sort_by(df, desc: x)

      assert Series.to_list(DataFrame.pull(result, "x")) == [3, 2, 1]
    end
  end

  describe "distinct" do
    test "removes duplicate rows" do
      df = DataFrame.new(x: [1, 1, 2, 2, 3])
      result = DataFrame.distinct(df)

      assert DataFrame.n_rows(result) == 3
    end
  end

  describe "join" do
    test "inner join" do
      left = DataFrame.new(id: [1, 2, 3], val: ["a", "b", "c"])
      right = DataFrame.new(id: [2, 3, 4], score: [10, 20, 30])
      result = DataFrame.join(left, right, on: [{"id", "id"}])

      assert DataFrame.n_rows(result) == 2
    end
  end

  describe "concat_rows" do
    test "concatenates dataframes vertically" do
      df1 = DataFrame.new(x: [1, 2], y: ["a", "b"])
      df2 = DataFrame.new(x: [3, 4], y: ["c", "d"])
      result = DataFrame.concat_rows([df1, df2])

      assert DataFrame.n_rows(result) == 4
      assert Series.to_list(DataFrame.pull(result, "x")) == [1, 2, 3, 4]
    end
  end

  describe "drop_nil" do
    test "removes rows with nil values" do
      df = DataFrame.new(x: [1, nil, 3, nil, 5])
      result = DataFrame.drop_nil(df)

      assert DataFrame.n_rows(result) == 3
      assert Series.to_list(DataFrame.pull(result, "x")) == [1, 3, 5]
    end
  end

  describe "sql" do
    test "executes raw SQL" do
      df = DataFrame.new(x: [1, 2, 3], y: [10, 20, 30])
      result = DataFrame.sql(df, "SELECT x, y, x + y AS total FROM df", table_name: "df")

      assert "total" in DataFrame.names(result)
      assert Series.to_list(DataFrame.pull(result, "total")) == [11, 22, 33]
    end
  end

  describe "summarise" do
    test "without groups" do
      df = DataFrame.new(x: [1, 2, 3, 4, 5])

      result =
        DataFrame.summarise_with(df, fn row ->
          [total: Series.sum(row["x"]), avg: Series.mean(row["x"])]
        end)

      assert DataFrame.n_rows(result) == 1
    end
  end

  describe "csv io" do
    @tag :tmp_dir
    test "round-trips through CSV", %{tmp_dir: tmp_dir} do
      path = Path.join(tmp_dir, "test.csv")
      df = DataFrame.new(x: [1, 2, 3], y: ["a", "b", "c"])
      DataFrame.to_csv(df, path)

      loaded = DataFrame.from_csv!(path)
      assert DataFrame.n_rows(loaded) == 3
      assert DataFrame.names(loaded) == ["x", "y"]
    end
  end

  describe "parquet io" do
    @tag :tmp_dir
    test "round-trips through Parquet", %{tmp_dir: tmp_dir} do
      path = Path.join(tmp_dir, "test.parquet")
      df = DataFrame.new(x: [1, 2, 3], y: ["a", "b", "c"])
      DataFrame.to_parquet(df, path)

      loaded = DataFrame.from_parquet!(path)
      assert DataFrame.n_rows(loaded) == 3
      assert DataFrame.names(loaded) == ["x", "y"]
    end
  end
end
