defmodule ExplorerDuckDB.IOTest do
  use ExUnit.Case, async: true

  require Explorer.DataFrame
  alias Explorer.DataFrame
  alias Explorer.Series

  setup do
    Explorer.Backend.put(ExplorerDuckDB)
    :ok
  end

  # ============================================================
  # CSV
  # ============================================================

  describe "CSV file IO" do
    @tag :tmp_dir
    test "write and read back integers", %{tmp_dir: tmp_dir} do
      path = Path.join(tmp_dir, "ints.csv")
      df = DataFrame.new(a: [1, 2, 3], b: [4, 5, 6])
      DataFrame.to_csv(df, path)

      loaded = DataFrame.from_csv!(path)
      assert DataFrame.n_rows(loaded) == 3
      assert Series.to_list(DataFrame.pull(loaded, "a")) == [1, 2, 3]
      assert Series.to_list(DataFrame.pull(loaded, "b")) == [4, 5, 6]
    end

    @tag :tmp_dir
    test "write and read back floats", %{tmp_dir: tmp_dir} do
      path = Path.join(tmp_dir, "floats.csv")
      df = DataFrame.new(x: [1.1, 2.2, 3.3])
      DataFrame.to_csv(df, path)

      loaded = DataFrame.from_csv!(path)
      assert DataFrame.n_rows(loaded) == 3
      values = Series.to_list(DataFrame.pull(loaded, "x"))
      assert_in_delta hd(values), 1.1, 0.001
    end

    @tag :tmp_dir
    test "write and read back strings with special chars", %{tmp_dir: tmp_dir} do
      path = Path.join(tmp_dir, "strings.csv")
      df = DataFrame.new(name: ["hello, world", "it's me", "line\nbreak"])
      DataFrame.to_csv(df, path)

      loaded = DataFrame.from_csv!(path)
      assert DataFrame.n_rows(loaded) == 3
    end

    @tag :tmp_dir
    test "write and read back mixed types", %{tmp_dir: tmp_dir} do
      path = Path.join(tmp_dir, "mixed.csv")
      df = DataFrame.new(id: [1, 2, 3], name: ["Alice", "Bob", "Carol"], score: [85.5, 92.0, 78.3])
      DataFrame.to_csv(df, path)

      loaded = DataFrame.from_csv!(path)
      assert DataFrame.n_rows(loaded) == 3
      assert DataFrame.names(loaded) == ["id", "name", "score"]
    end

    @tag :tmp_dir
    test "write single-row dataframe", %{tmp_dir: tmp_dir} do
      path = Path.join(tmp_dir, "single.csv")
      df = DataFrame.new(x: [42])
      DataFrame.to_csv(df, path)

      assert File.exists?(path)
      loaded = DataFrame.from_csv!(path)
      assert DataFrame.n_rows(loaded) == 1
    end

    @tag :tmp_dir
    test "overwrite existing file", %{tmp_dir: tmp_dir} do
      path = Path.join(tmp_dir, "overwrite.csv")
      df1 = DataFrame.new(x: [1, 2])
      df2 = DataFrame.new(x: [10, 20, 30])

      DataFrame.to_csv(df1, path)
      DataFrame.to_csv(df2, path)

      loaded = DataFrame.from_csv!(path)
      assert DataFrame.n_rows(loaded) == 3
    end
  end

  describe "CSV in-memory" do
    test "dump_csv produces valid CSV" do
      df = DataFrame.new(a: [1, 2, 3], b: ["x", "y", "z"])
      {:ok, csv} = DataFrame.dump_csv(df)

      assert csv =~ "a"
      assert csv =~ "b"
      assert String.split(csv, "\n") |> length() >= 4
    end

    test "load_csv parses CSV string" do
      csv = "x,y\n1,a\n2,b\n3,c\n"
      {:ok, df} = DataFrame.load_csv(csv)

      assert DataFrame.n_rows(df) == 3
      assert "x" in DataFrame.names(df)
    end

    test "dump then load roundtrip" do
      original = DataFrame.new(x: [10, 20, 30], name: ["Alice", "Bob", "Carol"])
      {:ok, csv} = DataFrame.dump_csv(original)
      {:ok, loaded} = DataFrame.load_csv(csv)

      assert DataFrame.n_rows(loaded) == DataFrame.n_rows(original)
    end

    test "dump lazy dataframe forces compute" do
      df = DataFrame.new(x: [1, 2, 3])
      lazy = DataFrame.lazy(df)
      {:ok, csv} = DataFrame.dump_csv(lazy)
      assert csv =~ "x"
    end
  end

  # ============================================================
  # Parquet
  # ============================================================

  describe "Parquet file IO" do
    @tag :tmp_dir
    test "write and read back integers", %{tmp_dir: tmp_dir} do
      path = Path.join(tmp_dir, "ints.parquet")
      df = DataFrame.new(a: [1, 2, 3], b: [4, 5, 6])
      DataFrame.to_parquet(df, path)

      loaded = DataFrame.from_parquet!(path)
      assert DataFrame.n_rows(loaded) == 3
      assert Series.to_list(DataFrame.pull(loaded, "a")) == [1, 2, 3]
    end

    @tag :tmp_dir
    test "write and read back with nils", %{tmp_dir: tmp_dir} do
      path = Path.join(tmp_dir, "nils.parquet")
      df = DataFrame.new(x: [1, nil, 3, nil, 5])
      DataFrame.to_parquet(df, path)

      loaded = DataFrame.from_parquet!(path)
      assert Series.to_list(DataFrame.pull(loaded, "x")) == [1, nil, 3, nil, 5]
    end

    @tag :tmp_dir
    test "preserves column types", %{tmp_dir: tmp_dir} do
      path = Path.join(tmp_dir, "types.parquet")
      df = DataFrame.new(
        int_col: [1, 2, 3],
        float_col: [1.1, 2.2, 3.3],
        str_col: ["a", "b", "c"],
        bool_col: [true, false, true]
      )
      DataFrame.to_parquet(df, path)

      loaded = DataFrame.from_parquet!(path)
      assert DataFrame.n_rows(loaded) == 3
    end
  end

  describe "Parquet in-memory" do
    test "dump then load roundtrip" do
      original = DataFrame.new(x: [10, 20, 30], y: [1.1, 2.2, 3.3])
      {:ok, binary} = DataFrame.dump_parquet(original)
      assert is_binary(binary)

      {:ok, loaded} = DataFrame.load_parquet(binary)
      assert DataFrame.n_rows(loaded) == 3
    end
  end

  # ============================================================
  # NDJSON
  # ============================================================

  describe "NDJSON file IO" do
    @tag :tmp_dir
    test "write and read back", %{tmp_dir: tmp_dir} do
      path = Path.join(tmp_dir, "data.ndjson")
      df = DataFrame.new(x: [1, 2, 3], name: ["Alice", "Bob", "Carol"])
      DataFrame.to_ndjson(df, path)

      loaded = DataFrame.from_ndjson!(path)
      assert DataFrame.n_rows(loaded) == 3
    end
  end

  describe "NDJSON in-memory" do
    test "dump then load roundtrip" do
      original = DataFrame.new(x: [1, 2, 3])
      {:ok, ndjson} = DataFrame.dump_ndjson(original)
      assert is_binary(ndjson)

      {:ok, loaded} = DataFrame.load_ndjson(ndjson)
      assert DataFrame.n_rows(loaded) == 3
    end
  end

  # ============================================================
  # Cross-format
  # ============================================================

  describe "cross-format" do
    @tag :tmp_dir
    test "CSV -> Parquet -> CSV roundtrip", %{tmp_dir: tmp_dir} do
      csv_path = Path.join(tmp_dir, "data.csv")
      parquet_path = Path.join(tmp_dir, "data.parquet")
      csv_back_path = Path.join(tmp_dir, "back.csv")

      df = DataFrame.new(x: [1, 2, 3], y: ["a", "b", "c"])
      DataFrame.to_csv(df, csv_path)

      from_csv = DataFrame.from_csv!(csv_path)
      DataFrame.to_parquet(from_csv, parquet_path)

      from_parquet = DataFrame.from_parquet!(parquet_path)
      DataFrame.to_csv(from_parquet, csv_back_path)

      final = DataFrame.from_csv!(csv_back_path)
      assert DataFrame.n_rows(final) == 3
      assert Series.to_list(DataFrame.pull(final, "y")) == ["a", "b", "c"]
    end

    @tag :tmp_dir
    test "write filtered result to parquet", %{tmp_dir: tmp_dir} do
      path = Path.join(tmp_dir, "filtered.parquet")

      df = DataFrame.new(x: Enum.to_list(1..100))

      df
      |> DataFrame.filter_with(fn row -> Series.greater(row["x"], 90) end)
      |> DataFrame.to_parquet(path)

      loaded = DataFrame.from_parquet!(path)
      assert DataFrame.n_rows(loaded) == 10
    end
  end
end
