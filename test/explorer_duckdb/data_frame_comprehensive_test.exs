defmodule ExplorerDuckDB.DataFrameComprehensiveTest do
  use ExUnit.Case, async: true

  require Explorer.DataFrame
  alias Explorer.DataFrame
  alias Explorer.Series

  setup do
    Explorer.Backend.put(ExplorerDuckDB)
    :ok
  end

  # ============================================================
  # IO: dump/load CSV
  # ============================================================

  describe "dump/load csv" do
    test "round-trips through in-memory CSV" do
      df = DataFrame.new(x: [1, 2, 3], y: ["a", "b", "c"])
      {:ok, csv_binary} = DataFrame.dump_csv(df)
      assert is_binary(csv_binary)
      assert csv_binary =~ "x"

      {:ok, loaded} = DataFrame.load_csv(csv_binary)
      assert DataFrame.n_rows(loaded) == 3
    end
  end

  # ============================================================
  # IO: dump/load Parquet
  # ============================================================

  describe "dump/load parquet" do
    test "round-trips through in-memory Parquet" do
      df = DataFrame.new(x: [1, 2, 3], y: [1.1, 2.2, 3.3])
      {:ok, parquet_binary} = DataFrame.dump_parquet(df)
      assert is_binary(parquet_binary)

      {:ok, loaded} = DataFrame.load_parquet(parquet_binary)
      assert DataFrame.n_rows(loaded) == 3
    end
  end

  # ============================================================
  # IO: NDJSON
  # ============================================================

  describe "ndjson" do
    @tag :tmp_dir
    test "round-trips through NDJSON file", %{tmp_dir: tmp_dir} do
      path = Path.join(tmp_dir, "test.ndjson")
      df = DataFrame.new(x: [1, 2, 3], y: ["a", "b", "c"])
      DataFrame.to_ndjson(df, path)

      loaded = DataFrame.from_ndjson!(path)
      assert DataFrame.n_rows(loaded) == 3
    end

    test "dump/load ndjson" do
      df = DataFrame.new(x: [1, 2, 3])
      {:ok, ndjson_binary} = DataFrame.dump_ndjson(df)
      assert is_binary(ndjson_binary)

      {:ok, loaded} = DataFrame.load_ndjson(ndjson_binary)
      assert DataFrame.n_rows(loaded) == 3
    end
  end

  # ============================================================
  # Sample
  # ============================================================

  describe "sample" do
    test "samples n rows" do
      df = DataFrame.new(x: Enum.to_list(1..100))
      result = DataFrame.sample(df, 10)
      assert DataFrame.n_rows(result) == 10
    end

    test "samples fraction" do
      df = DataFrame.new(x: Enum.to_list(1..100))
      result = DataFrame.sample(df, 0.1)
      assert DataFrame.n_rows(result) == 10
    end
  end

  # ============================================================
  # Concat columns
  # ============================================================

  describe "concat_columns" do
    test "joins dataframes side by side" do
      df1 = DataFrame.new(a: [1, 2, 3])
      df2 = DataFrame.new(b: [4, 5, 6])
      result = DataFrame.concat_columns([df1, df2])

      assert "a" in DataFrame.names(result)
      assert "b" in DataFrame.names(result)
      assert DataFrame.n_rows(result) == 3
    end
  end

  # ============================================================
  # Correlation / Covariance
  # ============================================================

  describe "correlation" do
    test "computes pairwise correlation" do
      df = DataFrame.new(x: [1.0, 2.0, 3.0], y: [2.0, 4.0, 6.0])
      result = DataFrame.correlation(df)
      assert DataFrame.n_rows(result) == 1
    end
  end

  describe "covariance" do
    test "computes pairwise covariance" do
      df = DataFrame.new(x: [1.0, 2.0, 3.0], y: [2.0, 4.0, 6.0])
      result = DataFrame.covariance(df)
      assert DataFrame.n_rows(result) == 1
    end
  end

  # ============================================================
  # Group by + summarise (more thorough)
  # ============================================================

  describe "group_by + summarise" do
    test "with single group" do
      df = DataFrame.new(
        dept: ["Eng", "Eng", "Sales", "Sales"],
        salary: [100, 120, 80, 90]
      )

      result =
        df
        |> DataFrame.group_by("dept")
        |> DataFrame.summarise_with(fn row ->
          [avg_salary: Series.mean(row["salary"])]
        end)

      assert DataFrame.n_rows(result) == 2
    end

    test "multiple aggregations" do
      df = DataFrame.new(x: [1, 2, 3, 4, 5])

      result =
        DataFrame.summarise_with(df, fn row ->
          [
            total: Series.sum(row["x"]),
            average: Series.mean(row["x"]),
            minimum: Series.min(row["x"]),
            maximum: Series.max(row["x"])
          ]
        end)

      assert DataFrame.n_rows(result) == 1
      assert Series.to_list(DataFrame.pull(result, "minimum")) == [1]
      assert Series.to_list(DataFrame.pull(result, "maximum")) == [5]
    end
  end

  # ============================================================
  # Join (more thorough)
  # ============================================================

  describe "join variations" do
    test "left join" do
      left = DataFrame.new(id: [1, 2, 3], val: ["a", "b", "c"])
      right = DataFrame.new(id: [2, 3, 4], score: [10, 20, 30])
      result = DataFrame.join(left, right, on: [{"id", "id"}], how: :left)

      assert DataFrame.n_rows(result) == 3
    end

    test "cross join" do
      left = DataFrame.new(x: [1, 2])
      right = DataFrame.new(y: ["a", "b"])
      result = DataFrame.join(left, right, how: :cross)

      assert DataFrame.n_rows(result) == 4
    end
  end

  # ============================================================
  # Chained lazy operations
  # ============================================================

  describe "complex chains" do
    test "filter + mutate + sort + head" do
      df = DataFrame.new(
        name: ["Alice", "Bob", "Carol", "Dave", "Eve"],
        score: [85, 92, 78, 95, 88]
      )

      result =
        df
        |> DataFrame.filter_with(fn row -> Series.greater(row["score"], 80) end)
        |> DataFrame.mutate_with(fn row -> [grade: Series.divide(row["score"], 10)] end)
        |> DataFrame.sort_by(desc: score)
        |> DataFrame.head(3)

      assert DataFrame.n_rows(result) == 3
      scores = Series.to_list(DataFrame.pull(result, "score"))
      assert hd(scores) == 95
    end

    test "multiple filters compose correctly" do
      df = DataFrame.new(x: Enum.to_list(1..20))

      result =
        df
        |> DataFrame.filter_with(fn row -> Series.greater(row["x"], 5) end)
        |> DataFrame.filter_with(fn row -> Series.less(row["x"], 15) end)
        |> DataFrame.filter_with(fn row -> Series.not_equal(row["x"], 10) end)

      values = Series.to_list(DataFrame.pull(result, "x"))
      assert 10 not in values
      assert Enum.all?(values, fn v -> v > 5 and v < 15 end)
    end
  end

  # ============================================================
  # SQL passthrough
  # ============================================================

  describe "sql" do
    test "aggregation via sql" do
      df = DataFrame.new(dept: ["Eng", "Eng", "Sales"], val: [10, 20, 30])

      result = DataFrame.sql(df, """
        SELECT dept, SUM(val) AS total
        FROM tbl
        GROUP BY dept
        ORDER BY total DESC
      """, table_name: "tbl")

      assert DataFrame.n_rows(result) == 2
    end

    test "window functions via sql" do
      df = DataFrame.new(x: [1, 2, 3, 4, 5])

      result = DataFrame.sql(df, """
        SELECT x, SUM(x) OVER (ORDER BY x) AS cumsum
        FROM tbl
      """, table_name: "tbl")

      cumsum = Series.to_list(DataFrame.pull(result, "cumsum"))
      assert cumsum == [1, 3, 6, 10, 15]
    end
  end

  # ============================================================
  # Built-in datasets
  # ============================================================

  describe "datasets" do
    test "loads iris" do
      df = Explorer.Datasets.iris()
      assert DataFrame.n_rows(df) == 150
      assert "species" in DataFrame.names(df)
    end

    test "iris group_by + summarise" do
      result =
        Explorer.Datasets.iris()
        |> DataFrame.group_by("species")
        |> DataFrame.summarise_with(fn row ->
          [mean_sepal_length: Series.mean(row["sepal_length"])]
        end)

      assert DataFrame.n_rows(result) == 3
    end
  end
end
