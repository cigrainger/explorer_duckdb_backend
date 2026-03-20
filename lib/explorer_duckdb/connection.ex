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
  """

  use GenServer

  alias ExplorerDuckDB.Native

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
  """
  def use(conn) do
    db = GenServer.call(conn, :get_db)
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

  # ── Server callbacks ──

  @impl true
  def init(:memory) do
    case Native.db_open() do
      {:ok, db} -> {:ok, db}
      db when is_reference(db) -> {:ok, db}
      {:error, error} -> {:stop, error}
    end
  end

  def init(path) when is_binary(path) do
    case Native.db_open_path(path) do
      {:ok, db} -> {:ok, db}
      db when is_reference(db) -> {:ok, db}
      {:error, error} -> {:stop, error}
    end
  end

  @impl true
  def handle_call(:get_db, _from, db) do
    {:reply, db, db}
  end

  def handle_call({:execute, sql}, _from, db) do
    result =
      case Native.db_execute(db, sql) do
        :ok -> :ok
        {:ok, _} -> :ok
        {} -> :ok
        {:error, error} -> {:error, to_string(error)}
      end

    {:reply, result, db}
  end

  def handle_call({:query, sql}, _from, db) do
    result =
      case Native.df_query(db, sql) do
        {:ok, ref} -> {:ok, ExplorerDuckDB.Shared.create_dataframe!(ref)}
        ref when is_reference(ref) -> {:ok, ExplorerDuckDB.Shared.create_dataframe!(ref)}
        {:error, error} -> {:error, to_string(error)}
      end

    {:reply, result, db}
  end
end
