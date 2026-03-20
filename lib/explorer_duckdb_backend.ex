defmodule ExplorerDuckDB do
  @moduledoc """
  DuckDB backend for the Explorer data analysis library.

  ## Usage

  Set DuckDB as the default backend:

      config :explorer, default_backend: ExplorerDuckDB

  Or set it per-process:

      Explorer.Backend.put(ExplorerDuckDB)

  ## Connecting to databases

  By default, an in-memory database is created per process. You can connect
  to file-based or remote databases:

      # File-based (persisted to disk)
      ExplorerDuckDB.open("my_data.duckdb")

      # Read-only
      ExplorerDuckDB.open("my_data.duckdb", read_only: true)

      # Attach another database alongside the current one
      ExplorerDuckDB.attach("/path/to/other.duckdb", as: "other_db")

      # Query across attached databases
      DataFrame.sql(df, "SELECT * FROM other_db.my_table", table_name: "df")

      # Connect to MotherDuck (cloud DuckDB)
      ExplorerDuckDB.open("md:my_database?motherduck_token=<token>")

      # Connect to Postgres via DuckDB extension
      ExplorerDuckDB.execute("INSTALL postgres; LOAD postgres;")
      ExplorerDuckDB.execute("ATTACH 'postgres://user:pass@host/db' AS pg (TYPE POSTGRES)")
      df = ExplorerDuckDB.query("SELECT * FROM pg.public.users")

      # Read from S3 directly
      ExplorerDuckDB.execute("INSTALL httpfs; LOAD httpfs;")
      df = DataFrame.from_parquet!("s3://my-bucket/data.parquet")
  """

  alias ExplorerDuckDB.Native
  alias ExplorerDuckDB.Shared

  @doc """
  Open a DuckDB database for the current process.

  ## Options

    * `:read_only` - Open in read-only mode (default: `false`)

  ## Examples

      # In-memory (default)
      ExplorerDuckDB.open(:memory)

      # File-based
      ExplorerDuckDB.open("analytics.duckdb")

      # MotherDuck cloud
      ExplorerDuckDB.open("md:my_database")
  """
  def open(path \\ :memory, _opts \\ []) do
    db =
      case path do
        :memory ->
          case Native.db_open() do
            {:ok, db} -> db
            db when is_reference(db) -> db
          end

        path when is_binary(path) ->
          case Native.db_open_path(path) do
            {:ok, db} -> db
            db when is_reference(db) -> db
            {:error, error} -> raise RuntimeError, to_string(error)
          end
      end

    Process.put(:explorer_duckdb_db, db)
    db
  end

  @doc """
  Execute a raw SQL statement on the current DuckDB connection.
  Returns `:ok`. Use `query/1` for statements that return results.

  ## Examples

      ExplorerDuckDB.execute("INSTALL httpfs; LOAD httpfs;")
      ExplorerDuckDB.execute("SET memory_limit = '4GB'")
      ExplorerDuckDB.execute("CREATE TABLE t (x INTEGER, y VARCHAR)")
  """
  def execute(sql) when is_binary(sql) do
    db = Shared.get_db()

    case Native.db_execute(db, sql) do
      :ok -> :ok
      {:ok, _} -> :ok
      {} -> :ok
      {:error, error} -> raise RuntimeError, to_string(error)
    end
  end

  @doc """
  Execute a SQL query and return the result as a DataFrame.

  ## Examples

      df = ExplorerDuckDB.query("SELECT * FROM read_parquet('s3://bucket/data.parquet')")

      df = ExplorerDuckDB.query("SELECT * FROM pg.public.users WHERE active = true")

      df = ExplorerDuckDB.query("SELECT 42 AS answer, 'hello' AS greeting")
  """
  def query(sql) when is_binary(sql) do
    db = Shared.get_db()

    case Native.df_query(db, sql) do
      {:ok, ref} -> Shared.create_dataframe!(ref)
      ref when is_reference(ref) -> Shared.create_dataframe!(ref)
      {:error, error} -> raise RuntimeError, to_string(error)
    end
  end

  @doc """
  Attach another database to the current connection.

  ## Options

    * `:as` - Alias for the attached database (default: filename stem)
    * `:type` - Database type: `:duckdb`, `:postgres`, `:mysql`, `:sqlite` (default: `:duckdb`)
    * `:read_only` - Attach in read-only mode (default: `false`)

  ## Examples

      # Attach a DuckDB file
      ExplorerDuckDB.attach("other.duckdb", as: "other")

      # Attach a Postgres database
      ExplorerDuckDB.attach("postgres://user:pass@host/db", as: "pg", type: :postgres)

      # Query across databases
      df = ExplorerDuckDB.query("SELECT * FROM other.my_table")
  """
  def attach(path, opts \\ []) do
    alias_name = Keyword.get(opts, :as, Path.rootname(Path.basename(path)))
    type = Keyword.get(opts, :type)
    read_only = Keyword.get(opts, :read_only, false)

    type_clause = if type, do: " (TYPE #{type |> to_string() |> String.upcase()})", else: ""
    ro_clause = if read_only, do: " (READ_ONLY)", else: ""

    execute("ATTACH '#{String.replace(path, "'", "''")}' AS \"#{alias_name}\"#{type_clause}#{ro_clause}")
  end

  @doc """
  Install and load a DuckDB extension.

  ## Examples

      ExplorerDuckDB.install_extension("httpfs")
      ExplorerDuckDB.install_extension("postgres")
      ExplorerDuckDB.install_extension("spatial")
  """
  def install_extension(name) when is_binary(name) do
    execute("INSTALL #{name}; LOAD #{name};")
  end

  @doc """
  Get the current DuckDB database reference for the process.
  Opens an in-memory database if none exists.
  """
  def current_db, do: Shared.get_db()

  @doc """
  Execute a SQL query and stream results as a sequence of DataFrames.
  Each element is a batch of rows (typically 2048, DuckDB's vector size).

  ## Examples

      ExplorerDuckDB.stream_query("SELECT * FROM large_table")
      |> Stream.each(fn batch_df ->
        IO.inspect(Explorer.DataFrame.n_rows(batch_df), label: "batch rows")
      end)
      |> Stream.run()
  """
  def stream_query(sql) when is_binary(sql) do
    db = Shared.get_db()

    {stream, _total_batches} =
      case Native.df_query_stream_init(db, sql) do
        {:ok, result} -> result
        result when is_tuple(result) -> result
      end

    Stream.resource(
      fn -> stream end,
      fn stream ->
        case Native.df_query_stream_next(stream) do
          {:ok, ref} ->
            df = Shared.create_dataframe!(ref)
            {[df], stream}

          :done ->
            {:halt, stream}
        end
      end,
      fn _ -> :ok end
    )
  end

  @doc """
  Clean up all temporary tables created by this process.
  Call this when you're done with Explorer operations to free DuckDB resources.

  This is called automatically when using `with_db/2`.
  """
  def cleanup do
    tables = Process.get(:explorer_duckdb_temp_tables, MapSet.new())

    if MapSet.size(tables) > 0 do
      db = Shared.get_db()
      sql = tables |> Enum.map_join("; ", &"DROP TABLE IF EXISTS #{&1}")
      Native.db_execute(db, sql)
      Process.put(:explorer_duckdb_temp_tables, MapSet.new())
    end

    :ok
  end

  @doc """
  Execute a block with a DuckDB database, cleaning up temp tables afterward.

  ## Examples

      ExplorerDuckDB.with_db fn ->
        df = DataFrame.from_csv!("data.csv")
        df |> DataFrame.filter(x > 10) |> DataFrame.to_csv("filtered.csv")
      end
  """
  def with_db(fun) do
    with_db(:memory, fun)
  end

  def with_db(path, fun) do
    open(path)
    Explorer.Backend.put(ExplorerDuckDB)

    try do
      fun.()
    after
      cleanup()
    end
  end
end
