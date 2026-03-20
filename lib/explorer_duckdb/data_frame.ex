defmodule ExplorerDuckDB.DataFrame do
  @moduledoc false

  @behaviour Explorer.Backend.DataFrame

  alias ExplorerDuckDB.Native
  alias ExplorerDuckDB.Shared
  alias ExplorerDuckDB.LazyFrame
  alias Explorer.Series

  defstruct resource: nil

  @type t :: %__MODULE__{resource: reference()}

  # ============================================================
  # IO: CSV
  # ============================================================

  @impl true
  def from_csv(
        entry,
        _dtypes,
        _delimiter,
        _nil_values,
        _skip_rows,
        _skip_rows_after_header,
        _header?,
        _encoding,
        _max_rows,
        _columns,
        _infer_schema_length,
        _parse_dates,
        _eol_delimiter
      ) do
    db = Shared.get_db()
    path = entry_to_path(entry)

    case Native.df_from_csv(db, path) do
      {:ok, df_ref} -> Shared.create_dataframe(df_ref)
      {:error, error} -> {:error, RuntimeError.exception(to_string(error))}
      df_ref -> Shared.create_dataframe(df_ref)
    end
  end

  @impl true
  def to_csv(df, entry, _header?, _delimiter, _quote_style, _streaming) do
    db = Shared.get_db()
    path = entry_to_path(entry)

    case Native.df_to_csv(db, df.data.resource, path) do
      {:ok, _} -> :ok
      :ok -> :ok
      {} -> :ok
      {:error, error} -> {:error, RuntimeError.exception(to_string(error))}
    end
  end

  @impl true
  def dump_csv(df, _header?, _delimiter, _quote_style) do
    path = temp_path("csv")

    try do
      to_csv(df, path, true, ",", :necessary, false)
      {:ok, File.read!(path)}
    after
      File.rm(path)
    end
  end

  @impl true
  def load_csv(
        contents, _dtypes, _delimiter, _nil_values, _skip_rows, _skip_rows_after_header,
        _header?, _encoding, _max_rows, _columns, _infer_schema_length, _parse_dates,
        _eol_delimiter
      ) do
    path = temp_path("csv")
    File.write!(path, contents)
    result = from_csv(path, [], ",", [], 0, 0, true, "utf-8", nil, nil, nil, false, nil)
    File.rm(path)
    result
  end

  # ============================================================
  # IO: Parquet
  # ============================================================

  @impl true
  def from_parquet(entry, _max_rows, _columns, _rechunk) do
    db = Shared.get_db()
    path = entry_to_path(entry)

    case Native.df_from_parquet(db, path) do
      {:ok, df_ref} -> Shared.create_dataframe(df_ref)
      {:error, error} -> {:error, RuntimeError.exception(to_string(error))}
      df_ref -> Shared.create_dataframe(df_ref)
    end
  end

  @impl true
  def to_parquet(df, entry, _compression, _streaming) do
    db = Shared.get_db()
    path = entry_to_path(entry)

    case Native.df_to_parquet(db, df.data.resource, path) do
      {:ok, _} -> :ok
      :ok -> :ok
      {} -> :ok
      {:error, error} -> {:error, RuntimeError.exception(to_string(error))}
    end
  end

  @impl true
  def dump_parquet(df, _compression) do
    path = temp_path("parquet")

    try do
      to_parquet(df, path, {nil, nil}, false)
      {:ok, File.read!(path)}
    after
      File.rm(path)
    end
  end

  @impl true
  def load_parquet(contents) do
    path = temp_path("parquet")
    File.write!(path, contents)
    result = from_parquet(path, nil, nil, false)
    File.rm(path)
    result
  end

  # ============================================================
  # IO: NDJSON
  # ============================================================

  @impl true
  def from_ndjson(entry, _infer_schema_length, _batch_size) do
    db = Shared.get_db()
    path = entry_to_path(entry)
    sql = "SELECT * FROM read_json_auto('#{String.replace(path, "'", "''")}')"

    case Native.df_query(db, sql) do
      {:ok, df_ref} -> Shared.create_dataframe(df_ref)
      {:error, error} -> {:error, RuntimeError.exception(to_string(error))}
      df_ref -> Shared.create_dataframe(df_ref)
    end
  end

  @impl true
  def to_ndjson(df, entry) do
    db = Shared.get_db()
    path = entry_to_path(entry)
    table = register_df(db, df)
    sql = "COPY #{table} TO '#{String.replace(path, "'", "''")}' (FORMAT JSON)"
    execute!(db, sql)
    cleanup_tables(db, [table])
    :ok
  end

  @impl true
  def dump_ndjson(df) do
    path = temp_path("ndjson")

    try do
      to_ndjson(df, path)
      {:ok, File.read!(path)}
    after
      File.rm(path)
    end
  end

  @impl true
  def load_ndjson(contents, _infer_schema_length, _batch_size) do
    path = temp_path("ndjson")
    File.write!(path, contents)
    result = from_ndjson(path, nil, nil)
    File.rm(path)
    result
  end

  # ============================================================
  # IO: IPC (not supported by DuckDB natively -- use temp Parquet as bridge)
  # ============================================================

  @impl true
  def from_ipc(_e, _c), do: {:error, RuntimeError.exception("IPC not supported by DuckDB backend")}
  @impl true
  def to_ipc(_df, _e, _c, _s), do: {:error, RuntimeError.exception("IPC not supported by DuckDB backend")}
  @impl true
  def dump_ipc(_df, _c), do: {:error, RuntimeError.exception("IPC not supported by DuckDB backend")}
  @impl true
  def load_ipc(_c, _cols), do: {:error, RuntimeError.exception("IPC not supported by DuckDB backend")}
  @impl true
  def dump_ipc_schema(_df, _cl), do: {:error, RuntimeError.exception("IPC not supported by DuckDB backend")}
  @impl true
  def dump_ipc_record_batch(_df, _i, _c, _cl),
    do: {:error, RuntimeError.exception("IPC not supported by DuckDB backend")}

  @impl true
  def from_ipc_stream(_e, _c), do: {:error, RuntimeError.exception("IPC Stream not supported by DuckDB backend")}
  @impl true
  def to_ipc_stream(_df, _e, _c), do: {:error, RuntimeError.exception("IPC Stream not supported by DuckDB backend")}
  @impl true
  def dump_ipc_stream(_df, _c), do: {:error, RuntimeError.exception("IPC Stream not supported by DuckDB backend")}
  @impl true
  def load_ipc_stream(_c, _cols), do: {:error, RuntimeError.exception("IPC Stream not supported by DuckDB backend")}

  @impl true
  def from_query(_c, _q, _p), do: {:error, RuntimeError.exception("ADBC not yet supported by DuckDB backend")}

  # ============================================================
  # Conversion
  # ============================================================

  @impl true
  def lazy, do: ExplorerDuckDB.LazyFrame

  @impl true
  def lazy(df), do: LazyFrame.from_eager(df)

  @impl true
  def compute(df), do: df

  @impl true
  def owner_reference(_df), do: nil
  @impl true
  def owner_import(_term), do: {:error, RuntimeError.exception("not supported")}
  @impl true
  def owner_export(_df), do: {:error, RuntimeError.exception("not supported")}

  @impl true
  def from_tabular(tabular, dtypes) do
    columns = Table.to_columns(tabular)

    series_list =
      Enum.map(columns, fn {name, values} ->
        name = to_string(name)
        dtype = Map.get(Map.new(dtypes), name)
        values = Enum.to_list(values)
        dtype = dtype || infer_dtype(values)
        {name, ExplorerDuckDB.Series.from_list_for_df(values, dtype, name)}
      end)

    from_series(series_list)
  end

  @impl true
  def from_series(series_list) do
    db = Shared.get_db()

    col_defs =
      Enum.map(series_list, fn {name, series} ->
        {name, series}
      end)

    case build_dataframe_from_series(db, col_defs) do
      {:ok, df_ref} -> Shared.create_dataframe!(df_ref)
      {:error, error} -> raise RuntimeError, to_string(error)
    end
  end

  @impl true
  def to_rows(df, _atom_keys?) do
    case Native.df_to_rows(df.data.resource) do
      {:ok, rows} -> rows
      rows when is_list(rows) -> rows
      {:error, error} -> raise RuntimeError, to_string(error)
    end
  end

  @impl true
  def to_rows_stream(df, atom_keys?, chunk_size) do
    rows = to_rows(df, atom_keys?)
    Stream.chunk_every(rows, chunk_size)
  end

  # ============================================================
  # Introspection
  # ============================================================

  @impl true
  def n_rows(df) do
    case Native.df_n_rows(df.data.resource) do
      {:ok, n} -> n
      n when is_integer(n) -> n
    end
  end

  @impl true
  def estimated_size(_df), do: 0

  @impl true
  def inspect(df, opts) do
    n_rows = n_rows(df)
    Explorer.Backend.DataFrame.inspect(df, "DuckDB", n_rows, opts)
  end

  @impl true
  def re_dtype(_regex), do: :string

  @impl true
  def pull(df, column) do
    case Native.df_pull(df.data.resource, column) do
      {:ok, series_ref} -> Shared.create_series(series_ref)
      series_ref when is_reference(series_ref) -> Shared.create_series(series_ref)
      {:error, error} -> raise RuntimeError, to_string(error)
    end
  end

  # ============================================================
  # Table verbs -- ALL delegate to lazy pipeline
  # ============================================================

  @impl true
  def head(df, rows) do
    df |> lazy() |> LazyFrame.head(rows) |> LazyFrame.compute_to_eager()
  end

  @impl true
  def tail(df, rows) do
    df |> lazy() |> LazyFrame.tail(rows) |> LazyFrame.compute_to_eager()
  end

  @impl true
  def select(df, out_df) do
    df |> lazy() |> LazyFrame.select(out_df) |> LazyFrame.compute_to_eager()
  end

  @impl true
  def mask(df, mask_series) do
    # mask is complex -- compute eagerly and filter in Elixir
    db = Shared.get_db()
    table = register_df(db, df)
    mask_table = register_series_as_table(db, mask_series)

    try do
      sql =
        "WITH numbered AS (SELECT *, ROW_NUMBER() OVER () AS __rn FROM #{table}) " <>
          "SELECT #{Enum.map_join(df.names, ", ", &~s(numbered."#{&1}"))} FROM numbered " <>
          "JOIN (SELECT ROW_NUMBER() OVER () AS __rn, \"value\" FROM #{mask_table}) m " <>
          "ON numbered.__rn = m.__rn WHERE m.\"value\" = true"

      query_raw(db, sql)
    after
      cleanup_tables(db, [table, mask_table])
    end
  end

  @impl true
  def filter_with(df, out_df, lazy_series) do
    df |> lazy() |> LazyFrame.filter_with(out_df, lazy_series) |> LazyFrame.compute_to_eager()
  end

  @impl true
  def mutate_with(df, out_df, mutations) do
    df |> lazy() |> LazyFrame.mutate_with(out_df, mutations) |> LazyFrame.compute_to_eager()
  end

  @impl true
  def sort_with(df, out_df, directions, maintain_order?, multithreaded?, nulls_last?) do
    df
    |> lazy()
    |> LazyFrame.sort_with(out_df, directions, maintain_order?, multithreaded?, nulls_last?)
    |> LazyFrame.compute_to_eager()
  end

  @impl true
  def distinct(df, out_df, columns) do
    df |> lazy() |> LazyFrame.distinct(out_df, columns) |> LazyFrame.compute_to_eager()
  end

  @impl true
  def rename(df, out_df, renames) do
    df |> lazy() |> LazyFrame.rename(out_df, renames) |> LazyFrame.compute_to_eager()
  end

  @impl true
  def dummies(df, _out_df, columns) do
    df |> lazy() |> LazyFrame.dummies(nil, columns) |> LazyFrame.compute_to_eager()
  end

  @impl true
  def sample(df, n_or_frac, replace, shuffle, seed) do
    df |> lazy() |> LazyFrame.sample(n_or_frac, replace, shuffle, seed) |> LazyFrame.compute_to_eager()
  end

  @impl true
  def slice(df, %Range{} = range) do
    list = to_rows(df, false)
    selected = Enum.slice(list, range)
    # Rebuild from rows -- simplified approach
    if selected == [] do
      %{df | data: %__MODULE__{resource: df.data.resource}}
    else
      from_tabular(selected, Enum.map(df.names, &{&1, df.dtypes[&1]}))
    end
  end

  def slice(df, indices) when is_list(indices) do
    db = Shared.get_db()
    table = register_df(db, df)

    sql =
      "WITH numbered AS (SELECT *, ROW_NUMBER() OVER () - 1 AS __rn FROM #{table}) " <>
        "SELECT #{Enum.map_join(df.names, ", ", &~s("#{&1}"))} FROM numbered " <>
        "WHERE __rn IN (#{Enum.join(indices, ", ")})"

    result = query_raw(db, sql)
    cleanup_tables(db, [table])
    result
  end

  @impl true
  def slice(df, offset, length) do
    df |> lazy() |> LazyFrame.slice(offset, length) |> LazyFrame.compute_to_eager()
  end

  @impl true
  def drop_nil(df, columns) do
    df |> lazy() |> LazyFrame.drop_nil(columns) |> LazyFrame.compute_to_eager()
  end

  @impl true
  def pivot_wider(df, id_columns, names_from, values_from, names_prefix) do
    df |> lazy() |> LazyFrame.pivot_wider(id_columns, names_from, values_from, names_prefix)
  end

  @impl true
  def pivot_longer(df, out_df, columns_to_pivot, columns_to_keep, names_to, values_to) do
    df |> lazy() |> LazyFrame.pivot_longer(out_df, columns_to_pivot, columns_to_keep, names_to, values_to)
  end

  @impl true
  def transpose(df, out_df, keep_names_as, new_column_names) do
    df |> lazy() |> LazyFrame.transpose(out_df, keep_names_as, new_column_names)
  end

  @impl true
  def put(df, out_df, column_name, series) do
    df
    |> lazy()
    |> LazyFrame.put(out_df, column_name, series)
    |> LazyFrame.compute_to_eager()
  end

  @impl true
  def nil_count(df) do
    df |> lazy() |> LazyFrame.nil_count() |> identity()
  end

  @impl true
  def explode(df, out_df, columns) do
    df |> lazy() |> LazyFrame.explode(out_df, columns) |> LazyFrame.compute_to_eager()
  end

  @impl true
  def unnest(df, out_df, columns) do
    df |> lazy() |> LazyFrame.unnest(out_df, columns) |> LazyFrame.compute_to_eager()
  end

  @impl true
  def correlation(df, out_df, method) do
    df |> lazy() |> LazyFrame.correlation(out_df, method) |> LazyFrame.compute_to_eager()
  end

  @impl true
  def covariance(df, out_df, ddof) do
    df |> lazy() |> LazyFrame.covariance(out_df, ddof) |> LazyFrame.compute_to_eager()
  end

  # ============================================================
  # Multi-table verbs
  # ============================================================

  @impl true
  def join(dfs, out_df, on, how) do
    LazyFrame.join(dfs, out_df, on, how) |> LazyFrame.compute_to_eager()
  end

  @impl true
  def concat_columns(dfs, out_df) do
    LazyFrame.concat_columns(dfs, out_df)
  end

  @impl true
  def concat_rows(dfs, out_df) do
    LazyFrame.concat_rows(dfs, out_df)
  end

  # ============================================================
  # Groups
  # ============================================================

  @impl true
  def summarise_with(df, out_df, aggregations) do
    df |> lazy() |> LazyFrame.summarise_with(out_df, aggregations)
  end

  # ============================================================
  # SQL
  # ============================================================

  @impl true
  def sql(df, sql_string, table_name) do
    df |> lazy() |> LazyFrame.sql(sql_string, table_name)
  end

  # ============================================================
  # Public helpers (used by LazyFrame)
  # ============================================================

  @doc false
  def register_df(db, df) do
    table_name = temp_name("df")

    # Try the NIF-based registration first (uses parameterized queries)
    case Native.df_register_table(db, df.data.resource, table_name) do
      {} -> table_name
      :ok -> table_name
      {:ok, _} -> table_name
      {:error, _error} ->
        # Fallback to SQL-based registration
        execute!(db, create_table_sql(df, table_name))
        insert_df_data(db, df, table_name)
        table_name
    end
  end

  # ============================================================
  # Private helpers
  # ============================================================

  defp identity(x), do: x

  defp entry_to_path(path) when is_binary(path), do: path
  defp entry_to_path(%{path: path}) when is_binary(path), do: path
  defp entry_to_path({:file, path}), do: path
  defp entry_to_path(other), do: raise("unsupported entry type: #{inspect(other)}")

  defp query_raw(db, sql) do
    case Native.df_query(db, sql) do
      {:ok, df_ref} -> Shared.create_dataframe!(df_ref)
      {:error, error} -> raise RuntimeError, to_string(error)
      df_ref -> Shared.create_dataframe!(df_ref)
    end
  end

  defp register_series_as_table(db, %Series{} = series) do
    table_name = temp_name("s")
    dtype_sql = explorer_dtype_to_duckdb_sql(series.dtype)
    execute!(db, "CREATE TEMPORARY TABLE #{table_name} (\"value\" #{dtype_sql})")

    values = Explorer.Series.to_list(series)
    insert_values(db, table_name, values)
    table_name
  end

  defp create_table_sql(df, table_name) do
    col_defs =
      Enum.map_join(df.names, ", ", fn name ->
        dtype = Map.get(df.dtypes, name)
        ~s("#{name}" #{explorer_dtype_to_duckdb_sql(dtype)})
      end)

    "CREATE TEMPORARY TABLE IF NOT EXISTS #{table_name} (#{col_defs})"
  end

  defp insert_df_data(db, df, table_name) do
    rows =
      case Native.df_to_rows(df.data.resource) do
        {:ok, rows} -> rows
        rows when is_list(rows) -> rows
      end

    if rows != [] do
      columns = df.names

      values_sql =
        Enum.map_join(rows, ", ", fn row ->
          vals = Enum.map_join(columns, ", ", fn col -> value_to_sql(Map.get(row, col)) end)
          "(#{vals})"
        end)

      col_names = Enum.map_join(columns, ", ", &~s("#{&1}"))
      execute!(db, "INSERT INTO #{table_name} (#{col_names}) VALUES #{values_sql}")
    end
  end

  defp insert_values(db, table_name, values) do
    if values != [] do
      values_sql = Enum.map_join(values, ", ", fn v -> "(#{value_to_sql(v)})" end)
      execute!(db, "INSERT INTO #{table_name} (\"value\") VALUES #{values_sql}")
    end
  end

  @doc false
  def execute!(db, sql) do
    case Native.db_execute(db, sql) do
      :ok -> :ok
      {:ok, _} -> :ok
      {} -> :ok
      {:error, error} -> raise RuntimeError, to_string(error)
    end
  end

  @doc false
  def cleanup_tables(db, tables) do
    sql = Enum.map_join(tables, "; ", &"DROP TABLE IF EXISTS #{&1}")
    Native.db_execute(db, sql)
    :ok
  end

  defp value_to_sql(nil), do: "NULL"
  defp value_to_sql(true), do: "TRUE"
  defp value_to_sql(false), do: "FALSE"
  defp value_to_sql(v) when is_integer(v), do: Integer.to_string(v)
  defp value_to_sql(v) when is_float(v), do: Float.to_string(v)
  defp value_to_sql(:nan), do: "'NaN'::DOUBLE"
  defp value_to_sql(:infinity), do: "'Infinity'::DOUBLE"
  defp value_to_sql(:neg_infinity), do: "'-Infinity'::DOUBLE"

  defp value_to_sql(v) when is_binary(v) do
    escaped = String.replace(v, "'", "''")
    "'#{escaped}'"
  end

  defp value_to_sql(%Date{} = d), do: "'#{Date.to_iso8601(d)}'::DATE"
  defp value_to_sql(%NaiveDateTime{} = dt), do: "'#{NaiveDateTime.to_iso8601(dt)}'::TIMESTAMP"
  defp value_to_sql(%DateTime{} = dt), do: "'#{DateTime.to_iso8601(dt)}'::TIMESTAMPTZ"
  defp value_to_sql(v), do: "'#{Elixir.Kernel.inspect(v)}'"

  def explorer_dtype_to_duckdb_sql({:s, 8}), do: "TINYINT"
  def explorer_dtype_to_duckdb_sql({:s, 16}), do: "SMALLINT"
  def explorer_dtype_to_duckdb_sql({:s, 32}), do: "INTEGER"
  def explorer_dtype_to_duckdb_sql({:s, 64}), do: "BIGINT"
  def explorer_dtype_to_duckdb_sql({:u, 8}), do: "UTINYINT"
  def explorer_dtype_to_duckdb_sql({:u, 16}), do: "USMALLINT"
  def explorer_dtype_to_duckdb_sql({:u, 32}), do: "UINTEGER"
  def explorer_dtype_to_duckdb_sql({:u, 64}), do: "UBIGINT"
  def explorer_dtype_to_duckdb_sql({:f, 32}), do: "FLOAT"
  def explorer_dtype_to_duckdb_sql({:f, 64}), do: "DOUBLE"
  def explorer_dtype_to_duckdb_sql(:boolean), do: "BOOLEAN"
  def explorer_dtype_to_duckdb_sql(:string), do: "VARCHAR"
  def explorer_dtype_to_duckdb_sql(:date), do: "DATE"
  def explorer_dtype_to_duckdb_sql(:time), do: "TIME"
  def explorer_dtype_to_duckdb_sql({:naive_datetime, _}), do: "TIMESTAMP"
  def explorer_dtype_to_duckdb_sql({:datetime, _, _}), do: "TIMESTAMPTZ"
  def explorer_dtype_to_duckdb_sql({:duration, _}), do: "INTERVAL"
  def explorer_dtype_to_duckdb_sql(:binary), do: "BLOB"
  def explorer_dtype_to_duckdb_sql({:decimal, p, s}), do: "DECIMAL(#{p}, #{s})"
  def explorer_dtype_to_duckdb_sql({:list, inner}), do: "#{explorer_dtype_to_duckdb_sql(inner)}[]"
  def explorer_dtype_to_duckdb_sql(:null), do: "INTEGER"
  def explorer_dtype_to_duckdb_sql(:category), do: "VARCHAR"
  def explorer_dtype_to_duckdb_sql(_), do: "VARCHAR"

  defp temp_name(prefix) do
    pid_hash = :erlang.phash2(self())
    id = System.unique_integer([:positive])
    "__explorer_#{prefix}_#{pid_hash}_#{id}"
  end

  @doc false
  def with_temp_table(db, df, fun) do
    table = register_df(db, df)

    try do
      fun.(table)
    after
      cleanup_tables(db, [table])
    end
  end

  @doc false
  def with_temp_tables(db, dfs, fun) do
    tables = Enum.map(dfs, &register_df(db, &1))

    try do
      fun.(tables)
    after
      cleanup_tables(db, tables)
    end
  end

  defp temp_path(ext) do
    dir = Path.join(System.tmp_dir!(), "explorer_duckdb")
    File.mkdir_p!(dir)
    Path.join(dir, "#{System.unique_integer([:positive])}.#{ext}")
  end

  defp infer_dtype([]), do: :string
  defp infer_dtype([nil | rest]), do: infer_dtype(rest)
  defp infer_dtype([val | _]) when is_integer(val), do: {:s, 64}
  defp infer_dtype([val | _]) when is_float(val), do: {:f, 64}
  defp infer_dtype([val | _]) when is_boolean(val), do: :boolean
  defp infer_dtype([val | _]) when is_binary(val), do: :string
  defp infer_dtype(_), do: :string

  defp build_dataframe_from_series(db, col_defs) do
    if col_defs == [] do
      {:error, "cannot create empty dataframe"}
    else
      tables_and_names =
        Enum.map(col_defs, fn {name, series} ->
          table = register_series_as_table(db, series)
          {table, name}
        end)

      select_parts =
        Enum.map(tables_and_names, fn {_table, name} ->
          "t_#{name}.\"value\" AS \"#{name}\""
        end)

      [{first_table, first_name} | rest] = tables_and_names

      from_clause =
        "(SELECT \"value\", ROW_NUMBER() OVER () AS __rn FROM #{first_table}) t_#{first_name}"

      join_clauses =
        Enum.map_join(rest, " ", fn {table, name} ->
          "JOIN (SELECT \"value\", ROW_NUMBER() OVER () AS __rn FROM #{table}) t_#{name} " <>
            "ON t_#{first_name}.__rn = t_#{name}.__rn"
        end)

      sql = "SELECT #{Enum.join(select_parts, ", ")} FROM #{from_clause} #{join_clauses}"

      result = Native.df_query(db, sql)
      cleanup_tables(db, Enum.map(tables_and_names, &elem(&1, 0)))
      {:ok, result}
    end
  end
end
