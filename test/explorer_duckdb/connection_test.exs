defmodule ExplorerDuckDB.ConnectionTest do
  use ExUnit.Case

  require Explorer.DataFrame
  alias Explorer.DataFrame
  alias Explorer.Series

  setup do
    Explorer.Backend.put(ExplorerDuckDB)
    # Reset to fresh in-memory db
    ExplorerDuckDB.open(:memory)
    :ok
  end

  describe "open" do
    test "opens in-memory database" do
      db = ExplorerDuckDB.open(:memory)
      assert is_reference(db)
    end

    @tag :tmp_dir
    test "opens file-based database", %{tmp_dir: tmp_dir} do
      path = Path.join(tmp_dir, "test.duckdb")
      db = ExplorerDuckDB.open(path)
      assert is_reference(db)

      # Create a table and insert data
      ExplorerDuckDB.execute("CREATE TABLE users (id INTEGER, name VARCHAR)")
      ExplorerDuckDB.execute("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')")

      # Query it
      df = ExplorerDuckDB.query("SELECT * FROM users ORDER BY id")
      assert DataFrame.n_rows(df) == 2
      assert Series.to_list(DataFrame.pull(df, "name")) == ["Alice", "Bob"]
    end

    @tag :tmp_dir
    test "persists data across opens", %{tmp_dir: tmp_dir} do
      path = Path.join(tmp_dir, "persist.duckdb")

      # Open and write
      ExplorerDuckDB.open(path)
      ExplorerDuckDB.execute("CREATE TABLE t (x INTEGER)")
      ExplorerDuckDB.execute("INSERT INTO t VALUES (42)")

      # Open again (same process, replaces connection)
      ExplorerDuckDB.open(path)
      df = ExplorerDuckDB.query("SELECT * FROM t")
      assert Series.to_list(DataFrame.pull(df, "x")) == [42]
    end
  end

  describe "execute" do
    test "runs DDL" do
      ExplorerDuckDB.execute("CREATE TABLE test_table (id INTEGER, val DOUBLE)")
      ExplorerDuckDB.execute("INSERT INTO test_table VALUES (1, 3.14)")

      df = ExplorerDuckDB.query("SELECT * FROM test_table")
      assert DataFrame.n_rows(df) == 1
    end

    test "configures DuckDB settings" do
      ExplorerDuckDB.execute("SET threads = 2")
      # Should not raise
    end

    test "raises on invalid SQL" do
      assert_raise RuntimeError, fn ->
        ExplorerDuckDB.execute("INVALID SQL SYNTAX HERE")
      end
    end
  end

  describe "query" do
    test "simple query" do
      df = ExplorerDuckDB.query("SELECT 1 AS a, 'hello' AS b")
      assert DataFrame.n_rows(df) == 1
      assert Series.to_list(DataFrame.pull(df, "a")) == [1]
      assert Series.to_list(DataFrame.pull(df, "b")) == ["hello"]
    end

    test "query with generate_series" do
      df = ExplorerDuckDB.query("SELECT * FROM generate_series(1, 100) AS t(x)")
      assert DataFrame.n_rows(df) == 100
    end

    test "query result can be used with Explorer API" do
      df = ExplorerDuckDB.query("SELECT * FROM generate_series(1, 50) AS t(x)")

      result =
        df
        |> DataFrame.filter_with(fn row -> Series.greater(row["x"], 25) end)
        |> DataFrame.sort_by(desc: x)
        |> DataFrame.head(5)

      assert Series.to_list(DataFrame.pull(result, "x")) == [50, 49, 48, 47, 46]
    end
  end

  describe "attach" do
    @tag :tmp_dir
    test "attaches another duckdb file", %{tmp_dir: tmp_dir} do
      # Create a database with data
      other_path = Path.join(tmp_dir, "other.duckdb")
      ExplorerDuckDB.open(other_path)
      ExplorerDuckDB.execute("CREATE TABLE items (name VARCHAR, price DOUBLE)")
      ExplorerDuckDB.execute("INSERT INTO items VALUES ('Widget', 9.99), ('Gadget', 19.99)")

      # Open a new in-memory db and attach the other one
      ExplorerDuckDB.open(:memory)
      ExplorerDuckDB.attach(other_path, as: "other")

      # Query across databases
      df = ExplorerDuckDB.query("SELECT * FROM other.items ORDER BY price")
      assert DataFrame.n_rows(df) == 2
      assert Series.to_list(DataFrame.pull(df, "name")) == ["Widget", "Gadget"]
    end
  end

  describe "install_extension" do
    test "installs json extension" do
      # json is built-in but install should be idempotent
      ExplorerDuckDB.install_extension("json")
      df = ExplorerDuckDB.query("SELECT json_extract('{\"a\": 1}', '$.a') AS val")
      assert DataFrame.n_rows(df) == 1
    end
  end

  describe "mixed Explorer + raw SQL workflow" do
    test "create table, populate with Explorer, query with SQL" do
      # Create an Explorer DataFrame
      df = DataFrame.new(
        name: ["Alice", "Bob", "Carol"],
        score: [85, 92, 78]
      )

      # Use SQL on it
      result = DataFrame.sql(df, """
        SELECT name, score,
               RANK() OVER (ORDER BY score DESC) AS rank
        FROM tbl
      """, table_name: "tbl")

      assert "rank" in DataFrame.names(result)
      assert DataFrame.n_rows(result) == 3
    end

    test "raw SQL table then Explorer operations" do
      ExplorerDuckDB.execute("""
        CREATE TABLE sales AS
        SELECT * FROM (VALUES
          ('2024-01', 'Widgets', 100),
          ('2024-01', 'Gadgets', 200),
          ('2024-02', 'Widgets', 150),
          ('2024-02', 'Gadgets', 250)
        ) AS t(month, product, revenue)
      """)

      df = ExplorerDuckDB.query("SELECT * FROM sales")

      result =
        df
        |> DataFrame.group_by("product")
        |> DataFrame.summarise_with(fn row ->
          [total: Series.sum(row["revenue"])]
        end)

      assert DataFrame.n_rows(result) == 2
    end
  end
end
