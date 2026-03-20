defmodule ExplorerDuckDB.Connection do
  @moduledoc """
  A GenServer that manages a shared DuckDB database connection.

  Use this when multiple processes need to share the same database:

      {:ok, conn} = ExplorerDuckDB.Connection.start_link(path: "my_data.duckdb")

      # Any process can use it
      ExplorerDuckDB.Connection.use(conn)
      df = Explorer.DataFrame.from_csv!("data.csv")

  The connection serializes all DuckDB operations through a single GenServer,
  which matches DuckDB's threading model (connections are not thread-safe).

  When a client process that called `use/1` exits, any temporary tables it
  created are automatically cleaned up.
  """

  use GenServer

  alias ExplorerDuckDB.Native

  defstruct [:db, monitors: %{}]

  # ── Client API ──

  @doc """
  Start a connection process.

  ## Options

    * `:path` - Database path (default: `:memory`)
    * `:name` - GenServer name for registration (optional)
  """
  def start_link(opts \\ []) do
    path = Keyword.get(opts, :path, :memory)
    gen_opts = if name = Keyword.get(opts, :name), do: [name: name], else: []
    GenServer.start_link(__MODULE__, path, gen_opts)
  end

  @doc """
  Set this connection as the DuckDB backend for the current process.
  All subsequent Explorer operations will use this shared connection.
  The connection will monitor this process and clean up temp tables on exit.
  """
  def use(conn) do
    db = GenServer.call(conn, {:use, self()})
    Process.put(:explorer_duckdb_db, db)
    Explorer.Backend.put(ExplorerDuckDB)
    :ok
  end

  @doc """
  Execute a SQL statement on the shared connection.
  """
  def execute(conn, sql) do
    GenServer.call(conn, {:execute, sql}, :infinity)
  end

  @doc """
  Run a SQL query and return a DataFrame.
  """
  def query(conn, sql) do
    GenServer.call(conn, {:query, sql}, :infinity)
  end

  @doc """
  Get the raw DuckDB reference.
  """
  def get_db(conn) do
    GenServer.call(conn, :get_db)
  end

  @doc """
  Clean up all temporary tables for a specific process.
  """
  def cleanup(conn, pid \\ self()) do
    GenServer.call(conn, {:cleanup, pid}, :infinity)
  end

  # ── Server callbacks ──

  @impl true
  def init(:memory) do
    case Native.db_open() do
      {:ok, db} -> {:ok, %__MODULE__{db: db}}
      db when is_reference(db) -> {:ok, %__MODULE__{db: db}}
      {:error, error} -> {:stop, error}
    end
  end

  def init(path) when is_binary(path) do
    case Native.db_open_path(path) do
      {:ok, db} -> {:ok, %__MODULE__{db: db}}
      db when is_reference(db) -> {:ok, %__MODULE__{db: db}}
      {:error, error} -> {:stop, error}
    end
  end

  @impl true
  def handle_call(:get_db, _from, state) do
    {:reply, state.db, state}
  end

  def handle_call({:use, pid}, _from, state) do
    state =
      if Map.has_key?(state.monitors, pid) do
        state
      else
        ref = Process.monitor(pid)
        %{state | monitors: Map.put(state.monitors, pid, ref)}
      end

    {:reply, state.db, state}
  end

  def handle_call({:execute, sql}, _from, state) do
    result =
      case Native.db_execute(state.db, sql) do
        :ok -> :ok
        {:ok, _} -> :ok
        {} -> :ok
        {:error, error} -> {:error, to_string(error)}
      end

    {:reply, result, state}
  end

  def handle_call({:query, sql}, _from, state) do
    result =
      case Native.df_query(state.db, sql) do
        {:ok, ref} -> {:ok, ExplorerDuckDB.Shared.create_dataframe!(ref)}
        ref when is_reference(ref) -> {:ok, ExplorerDuckDB.Shared.create_dataframe!(ref)}
        {:error, error} -> {:error, to_string(error)}
      end

    {:reply, result, state}
  end

  def handle_call({:cleanup, _pid}, _from, state) do
    # Clean up all temp tables matching the process pattern
    cleanup_process_tables(state.db)
    {:reply, :ok, state}
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, pid, _reason}, state) do
    # Process died -- clean up any temp tables it created
    # Tables are named __explorer_{type}_{pid_hash}_{id}
    pid_hash = :erlang.phash2(pid)
    cleanup_sql = "SELECT table_name FROM information_schema.tables WHERE table_name LIKE '__explorer_%_#{pid_hash}_%'"

    case Native.df_query(state.db, cleanup_sql) do
      {:ok, ref} -> drop_tables_from_query(state.db, ref)
      ref when is_reference(ref) -> drop_tables_from_query(state.db, ref)
      _ -> :ok
    end

    monitors = Map.delete(state.monitors, pid)
    {:noreply, %{state | monitors: monitors}}
  end

  defp drop_tables_from_query(db, ref) do
    case Native.df_to_rows(ref) do
      {:ok, rows} -> do_drop(db, rows)
      rows when is_list(rows) -> do_drop(db, rows)
      _ -> :ok
    end
  end

  defp do_drop(db, rows) do
    tables = Enum.map(rows, fn row -> row |> Map.values() |> hd() end)
    if tables != [] do
      sql = Enum.map_join(tables, "; ", &"DROP TABLE IF EXISTS #{&1}")
      Native.db_execute(db, sql)
    end
  end

  defp cleanup_process_tables(db) do
    pid_hash = :erlang.phash2(self())
    cleanup_sql = "SELECT table_name FROM information_schema.tables WHERE table_name LIKE '__explorer_%_#{pid_hash}_%'"

    case Native.df_query(db, cleanup_sql) do
      {:ok, ref} -> drop_tables_from_query(db, ref)
      ref when is_reference(ref) -> drop_tables_from_query(db, ref)
      _ -> :ok
    end
  end
end
