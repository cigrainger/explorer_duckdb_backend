defmodule ExplorerDuckDB.ConnectionGenServerTest do
  use ExUnit.Case

  alias ExplorerDuckDB.Connection
  alias Explorer.DataFrame
  alias Explorer.Series

  describe "shared connection" do
    test "start and use from current process" do
      {:ok, conn} = Connection.start_link()
      Connection.use(conn)

      df = DataFrame.new(x: [1, 2, 3])
      assert DataFrame.n_rows(df) == 3

      GenServer.stop(conn)
    end

    test "execute and query through connection" do
      {:ok, conn} = Connection.start_link()

      Connection.execute(conn, "CREATE TABLE test_conn (x INTEGER)")
      Connection.execute(conn, "INSERT INTO test_conn VALUES (1), (2), (3)")

      {:ok, df} = Connection.query(conn, "SELECT * FROM test_conn ORDER BY x")
      assert DataFrame.n_rows(df) == 3
      assert Series.to_list(DataFrame.pull(df, "x")) == [1, 2, 3]

      GenServer.stop(conn)
    end

    test "multiple processes share data" do
      {:ok, conn} = Connection.start_link()
      Connection.use(conn)

      # Write from this process
      ExplorerDuckDB.execute("CREATE TABLE shared (val INTEGER)")
      ExplorerDuckDB.execute("INSERT INTO shared VALUES (42)")

      # Read from another process
      task =
        Task.async(fn ->
          Connection.use(conn)
          ExplorerDuckDB.query("SELECT * FROM shared")
        end)

      df = Task.await(task)
      assert Series.to_list(DataFrame.pull(df, "val")) == [42]

      GenServer.stop(conn)
    end

    @tag :tmp_dir
    test "file-based shared connection", %{tmp_dir: tmp_dir} do
      path = Path.join(tmp_dir, "shared.duckdb")
      {:ok, conn} = Connection.start_link(path: path)

      Connection.execute(conn, "CREATE TABLE t (x INTEGER)")
      Connection.execute(conn, "INSERT INTO t VALUES (1), (2)")
      {:ok, df} = Connection.query(conn, "SELECT * FROM t")
      assert DataFrame.n_rows(df) == 2

      GenServer.stop(conn)
    end

    test "named connection" do
      {:ok, _conn} = Connection.start_link(name: :test_db)

      Connection.execute(:test_db, "CREATE TABLE named_test (x INTEGER)")
      Connection.execute(:test_db, "INSERT INTO named_test VALUES (99)")
      {:ok, df} = Connection.query(:test_db, "SELECT * FROM named_test")
      assert Series.to_list(DataFrame.pull(df, "x")) == [99]

      GenServer.stop(:test_db)
    end
  end
end
