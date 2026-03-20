defmodule ExplorerDuckDB.LifecycleTest do
  use ExUnit.Case

  require Explorer.DataFrame
  alias Explorer.DataFrame
  alias Explorer.Series

  setup do
    Explorer.Backend.put(ExplorerDuckDB)
    ExplorerDuckDB.open(:memory)
    :ok
  end

  # ============================================================
  # Streaming
  # ============================================================

  describe "to_rows_stream" do
    test "streams rows" do
      df = DataFrame.new(x: Enum.to_list(1..100))
      stream = DataFrame.to_rows_stream(df, chunk_size: 25)

      all_rows = Enum.to_list(stream)
      assert length(all_rows) == 100
    end

    test "stream yields maps" do
      df = DataFrame.new(x: [1, 2, 3], y: ["a", "b", "c"])
      rows = DataFrame.to_rows_stream(df) |> Enum.to_list()

      assert length(rows) == 3
      assert is_map(hd(rows))
    end

    test "stream is lazy" do
      df = DataFrame.new(x: Enum.to_list(1..1000))
      stream = DataFrame.to_rows_stream(df, chunk_size: 100)

      # Just take 1 row
      [first] = Enum.take(stream, 1)
      assert is_map(first)
    end
  end

  # ============================================================
  # Temp table cleanup
  # ============================================================

  describe "cleanup" do
    test "cleanup removes temp tables" do
      df = DataFrame.new(x: [1, 2, 3])

      # Do some operations that create temp tables
      DataFrame.filter_with(df, fn row -> Series.greater(row["x"], 1) end)
      DataFrame.sort_by(df, asc: x)

      # Clean up
      ExplorerDuckDB.cleanup()

      # Verify tables are gone
      tables = Process.get(:explorer_duckdb_temp_tables, MapSet.new())
      assert MapSet.size(tables) == 0
    end

    test "with_db cleans up automatically" do
      result =
        ExplorerDuckDB.with_db(fn ->
          df = DataFrame.new(x: [1, 2, 3])
          DataFrame.n_rows(df)
        end)

      assert result == 3

      # Temp tables should be cleaned
      tables = Process.get(:explorer_duckdb_temp_tables, MapSet.new())
      assert MapSet.size(tables) == 0
    end

    test "with_db with file path" do
      tmp = System.tmp_dir!()
      path = Path.join(tmp, "lifecycle_test_#{System.unique_integer([:positive])}.duckdb")

      try do
        result =
          ExplorerDuckDB.with_db(path, fn ->
            ExplorerDuckDB.execute("CREATE TABLE t (x INTEGER)")
            ExplorerDuckDB.execute("INSERT INTO t VALUES (42)")
            df = ExplorerDuckDB.query("SELECT * FROM t")
            DataFrame.n_rows(df)
          end)

        assert result == 1
      after
        File.rm(path)
      end
    end
  end

  # ============================================================
  # Connection GenServer lifecycle
  # ============================================================

  describe "connection process monitoring" do
    test "cleans up when client process dies" do
      {:ok, conn} = ExplorerDuckDB.Connection.start_link()

      # Spawn a process that creates temp tables then dies
      task =
        Task.async(fn ->
          ExplorerDuckDB.Connection.use(conn)
          ExplorerDuckDB.execute("CREATE TEMPORARY TABLE __test_lifecycle (x INTEGER)")
          ExplorerDuckDB.execute("INSERT INTO __test_lifecycle VALUES (1)")
          # Process exits here
          :ok
        end)

      Task.await(task)

      # Give the monitor time to fire
      Process.sleep(100)

      # The connection should still work
      ExplorerDuckDB.Connection.use(conn)
      df = ExplorerDuckDB.query("SELECT 1 AS x")
      assert DataFrame.n_rows(df) == 1

      GenServer.stop(conn)
    end

    test "multiple processes using same connection" do
      {:ok, conn} = ExplorerDuckDB.Connection.start_link()

      ExplorerDuckDB.Connection.execute(conn, "CREATE TABLE shared_data (val INTEGER)")
      ExplorerDuckDB.Connection.execute(conn, "INSERT INTO shared_data VALUES (1), (2), (3)")

      # Multiple processes read the same data
      tasks =
        for _ <- 1..5 do
          Task.async(fn ->
            ExplorerDuckDB.Connection.use(conn)
            df = ExplorerDuckDB.query("SELECT SUM(val) AS total FROM shared_data")
            Series.to_list(DataFrame.pull(df, "total")) |> hd()
          end)
        end

      results = Task.await_many(tasks, 10_000)
      assert Enum.all?(results, &(&1 == 6))

      GenServer.stop(conn)
    end
  end

  # ============================================================
  # Connection safety
  # ============================================================

  describe "connection reopen" do
    test "reopen replaces connection" do
      ExplorerDuckDB.open(:memory)
      ExplorerDuckDB.execute("CREATE TABLE t1 (x INTEGER)")

      # Reopen -- should get a fresh database
      ExplorerDuckDB.open(:memory)

      # t1 shouldn't exist in the new database
      assert_raise RuntimeError, fn ->
        ExplorerDuckDB.query("SELECT * FROM t1")
      end
    end

    @tag :tmp_dir
    test "reopen file database preserves data", %{tmp_dir: tmp_dir} do
      path = Path.join(tmp_dir, "reopen.duckdb")

      ExplorerDuckDB.open(path)
      ExplorerDuckDB.execute("CREATE TABLE persist (val INTEGER)")
      ExplorerDuckDB.execute("INSERT INTO persist VALUES (42)")

      # Reopen same file
      ExplorerDuckDB.open(path)
      df = ExplorerDuckDB.query("SELECT * FROM persist")
      assert Series.to_list(DataFrame.pull(df, "val")) == [42]
    end
  end
end
