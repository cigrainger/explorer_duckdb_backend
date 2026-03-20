defmodule ExplorerDuckDB.LazyFrame do
  @moduledoc false

  @behaviour Explorer.Backend.DataFrame

  alias ExplorerDuckDB.Native
  alias ExplorerDuckDB.Shared
  alias ExplorerDuckDB.QueryBuilder
  alias ExplorerDuckDB.SQLBuilder
  alias Explorer.Series

  defstruct [:db, :source, operations: [], groups: []]

  @type t :: %__MODULE__{
          db: reference(),
          source: String.t(),
          operations: list(),
          groups: list()
        }

  @doc """
  Create a LazyFrame from an eager DataFrame.
  Registers the eager data as a DuckDB temp table and returns a lazy handle.
  """
  def from_eager(df) do
    db = Shared.get_db()
    table_name = ExplorerDuckDB.DataFrame.register_df(db, df)
    groups = (df.groups[:columns] || [])

    lazy_data = %__MODULE__{
      db: db,
      source: table_name,
      operations: [],
      groups: groups
    }

    %{df | data: lazy_data}
  end

  @doc """
  Execute the accumulated operations and return an eager DataFrame.
  """
  def compute_to_eager(%Explorer.DataFrame{data: %ExplorerDuckDB.DataFrame{}} = df), do: df

  def compute_to_eager(%Explorer.DataFrame{data: %__MODULE__{} = lazy}) do
    sql = QueryBuilder.build(lazy.source, lazy.operations)
    db = lazy.db

    # Exclude __rowid from the result
    # If the query is a CTE (WITH ...), wrap it to exclude __rowid
    exclude_sql =
      if String.starts_with?(sql, "WITH ") do
        # Replace the final SELECT * with SELECT * EXCLUDE (__rowid)
        String.replace(sql, ~r/SELECT \* FROM (__s\d+)$/, "SELECT * EXCLUDE (__rowid) FROM \\1")
      else
        # Simple source -- wrap in subquery
        "SELECT * EXCLUDE (__rowid) FROM (SELECT * FROM #{sql})"
      end

    result =
      case Native.df_query(db, exclude_sql) do
        {:ok, ref} -> ref
        ref when is_reference(ref) -> ref
        {:error, _} ->
          # Fallback: no __rowid column
          fallback_sql =
            if String.starts_with?(sql, "WITH "), do: sql, else: "SELECT * FROM #{sql}"

          case Native.df_query(db, fallback_sql) do
            {:ok, ref} -> ref
            ref when is_reference(ref) -> ref
            {:error, error} -> raise RuntimeError, to_string(error)
          end
      end

    # Clean up source temp table
    cleanup(db, lazy.source)

    eager_df = Shared.create_dataframe!(result)
    # Preserve groups if any
    if lazy.groups != [] do
      %{eager_df | groups: %{columns: lazy.groups, stable?: false}}
    else
      eager_df
    end
  end

  # Push an operation onto the lazy frame, return updated df
  defp push(df, op) do
    %{df | data: %{df.data | operations: [op | df.data.operations]}}
  end

  defp cleanup(db, source) do
    Native.db_execute(db, "DROP TABLE IF EXISTS #{source}")
  end

  # ============================================================
  # Conversion
  # ============================================================

  @impl true
  def lazy, do: __MODULE__

  @impl true
  def lazy(df), do: df

  @impl true
  def compute(df), do: compute_to_eager(df)

  @impl true
  def owner_reference(_df), do: nil
  @impl true
  def owner_import(_term), do: {:error, RuntimeError.exception("not supported")}
  @impl true
  def owner_export(_df), do: {:error, RuntimeError.exception("not supported")}

  # Delegate IO and introspection to eager
  @impl true
  def from_tabular(tabular, dtypes), do: ExplorerDuckDB.DataFrame.from_tabular(tabular, dtypes)
  @impl true
  def from_series(series_list), do: ExplorerDuckDB.DataFrame.from_series(series_list)

  @impl true
  def to_rows(df, atom_keys?), do: compute(df) |> ExplorerDuckDB.DataFrame.to_rows(atom_keys?)
  @impl true
  def to_rows_stream(df, atom_keys?, chunk_size),
    do: compute(df) |> ExplorerDuckDB.DataFrame.to_rows_stream(atom_keys?, chunk_size)

  @impl true
  def n_rows(df), do: compute(df) |> ExplorerDuckDB.DataFrame.n_rows()
  @impl true
  def estimated_size(_df), do: 0
  @impl true
  def inspect(df, opts), do: compute(df) |> ExplorerDuckDB.DataFrame.inspect(opts)
  @impl true
  def re_dtype(regex), do: ExplorerDuckDB.DataFrame.re_dtype(regex)

  @impl true
  def pull(df, column), do: compute(df) |> ExplorerDuckDB.DataFrame.pull(column)

  # ============================================================
  # IO (delegate to eager)
  # ============================================================

  @impl true
  def from_csv(e, d, dl, nv, sr, sra, h, enc, mr, c, isl, pd, eol),
    do: ExplorerDuckDB.DataFrame.from_csv(e, d, dl, nv, sr, sra, h, enc, mr, c, isl, pd, eol)
  @impl true
  def to_csv(df, e, h, d, q, s), do: compute(df) |> ExplorerDuckDB.DataFrame.to_csv(e, h, d, q, s)
  @impl true
  def dump_csv(df, h, d, q), do: compute(df) |> ExplorerDuckDB.DataFrame.dump_csv(h, d, q)
  @impl true
  def load_csv(c, d, dl, nv, sr, sra, h, enc, mr, cols, isl, pd, eol),
    do: ExplorerDuckDB.DataFrame.load_csv(c, d, dl, nv, sr, sra, h, enc, mr, cols, isl, pd, eol)

  @impl true
  def from_parquet(e, mr, c, r), do: ExplorerDuckDB.DataFrame.from_parquet(e, mr, c, r)
  @impl true
  def to_parquet(df, e, c, s), do: compute(df) |> ExplorerDuckDB.DataFrame.to_parquet(e, c, s)
  @impl true
  def dump_parquet(df, c), do: compute(df) |> ExplorerDuckDB.DataFrame.dump_parquet(c)
  @impl true
  def load_parquet(c), do: ExplorerDuckDB.DataFrame.load_parquet(c)

  @impl true
  def from_ipc(e, c), do: ExplorerDuckDB.DataFrame.from_ipc(e, c)
  @impl true
  def to_ipc(df, e, c, s), do: compute(df) |> ExplorerDuckDB.DataFrame.to_ipc(e, c, s)
  @impl true
  def dump_ipc(df, c), do: compute(df) |> ExplorerDuckDB.DataFrame.dump_ipc(c)
  @impl true
  def load_ipc(c, cols), do: ExplorerDuckDB.DataFrame.load_ipc(c, cols)
  @impl true
  def dump_ipc_schema(df, cl), do: compute(df) |> ExplorerDuckDB.DataFrame.dump_ipc_schema(cl)
  @impl true
  def dump_ipc_record_batch(df, i, c, cl),
    do: compute(df) |> ExplorerDuckDB.DataFrame.dump_ipc_record_batch(i, c, cl)

  @impl true
  def from_ipc_stream(e, c), do: ExplorerDuckDB.DataFrame.from_ipc_stream(e, c)
  @impl true
  def to_ipc_stream(df, e, c), do: compute(df) |> ExplorerDuckDB.DataFrame.to_ipc_stream(e, c)
  @impl true
  def dump_ipc_stream(df, c), do: compute(df) |> ExplorerDuckDB.DataFrame.dump_ipc_stream(c)
  @impl true
  def load_ipc_stream(c, cols), do: ExplorerDuckDB.DataFrame.load_ipc_stream(c, cols)

  @impl true
  def from_ndjson(e, isl, bs), do: ExplorerDuckDB.DataFrame.from_ndjson(e, isl, bs)
  @impl true
  def to_ndjson(df, e), do: compute(df) |> ExplorerDuckDB.DataFrame.to_ndjson(e)
  @impl true
  def dump_ndjson(df), do: compute(df) |> ExplorerDuckDB.DataFrame.dump_ndjson()
  @impl true
  def load_ndjson(c, isl, bs), do: ExplorerDuckDB.DataFrame.load_ndjson(c, isl, bs)

  @impl true
  @compile {:no_warn_undefined, Adbc.Connection}
  def from_query(c, q, p), do: ExplorerDuckDB.DataFrame.from_query(c, q, p)

  # ============================================================
  # Table verbs (lazy -- accumulate operations)
  # ============================================================

  @impl true
  def head(df, rows), do: push(df, {:limit, rows})

  @impl true
  def tail(df, rows) do
    # Count rows from the current lazy state without consuming the source
    lazy = df.data
    sql = QueryBuilder.build(lazy.source, lazy.operations)

    count_sql =
      if String.starts_with?(sql, "WITH ") do
        # CTE query -- wrap to count
        String.replace(sql, ~r/SELECT \* FROM (__s\d+)$/, "SELECT COUNT(*) AS c FROM \\1")
      else
        "SELECT COUNT(*) AS c FROM #{sql}"
      end

    total =
      case Native.df_query(lazy.db, count_sql) do
        {:ok, ref} ->
          case Native.df_to_rows(ref) do
            {:ok, [%{"c" => n}]} -> n
            [%{"c" => n}] -> n
            _ -> 0
          end

        ref when is_reference(ref) ->
          case Native.df_to_rows(ref) do
            {:ok, [%{"c" => n}]} -> n
            [%{"c" => n}] -> n
            _ -> 0
          end
      end

    offset = max(total - rows, 0)
    push(df, {:limit, rows, offset})
  end

  @impl true
  def select(df, out_df), do: push(df, {:select, out_df.names})

  @impl true
  def mask(df, %Series{} = mask_series) do
    # mask requires materializing -- too complex for pure SQL accumulation
    compute(df) |> ExplorerDuckDB.DataFrame.mask(mask_series)
  end

  @impl true
  def filter_with(df, _out_df, %Explorer.Backend.LazySeries{} = lazy_series) do
    expr = SQLBuilder.to_sql(lazy_series)
    push(df, {:filter, expr})
  end

  @impl true
  def mutate_with(df, out_df, mutations) do
    mutation_exprs =
      Enum.map(mutations, fn
        {name, %Explorer.Backend.LazySeries{} = lazy} ->
          {name, SQLBuilder.to_sql(lazy)}

        {name, %Explorer.Series{} = series} ->
          {name, SQLBuilder.to_sql(series)}
      end)

    push(df, {:mutate, out_df.names, mutation_exprs})
  end

  @impl true
  def sort_with(df, _out_df, directions, _maintain_order?, _multithreaded?, nulls_last?) do
    sort_exprs =
      Enum.map(directions, fn {dir, lazy_series} ->
        {dir, SQLBuilder.to_sql(lazy_series), nulls_last?}
      end)

    push(df, {:sort, sort_exprs})
  end

  @impl true
  def distinct(df, _out_df, columns), do: push(df, {:distinct, columns})

  @impl true
  def rename(df, _out_df, renames) do
    push(df, {:rename, df.names, renames})
  end

  @impl true
  def dummies(df, _out_df, columns) do
    eager = compute_to_eager(df)

    # Build one-hot columns via mutate_with for each distinct value
    Enum.reduce(columns, eager, fn col, acc_df ->
      distinct_vals = acc_df |> Explorer.DataFrame.pull(col) |> Series.to_list() |> Enum.uniq()

      Enum.reduce(distinct_vals, acc_df, fn val, inner_df ->
        col_name = "#{col}_#{val}"

        inner_df
        |> Explorer.DataFrame.mutate_with(fn row ->
          [{col_name, Series.equal(row[col], val)}]
        end)
      end)
    end)
  end

  @impl true
  def sample(df, n_or_frac, _replace, _shuffle, _seed) do
    query_with_eager(df, fn db, table, eager ->
      n =
        if is_float(n_or_frac) do
          trunc(Explorer.DataFrame.n_rows(eager) * n_or_frac)
        else
          n_or_frac
        end

      query_sql(db, "SELECT * FROM #{table} ORDER BY RANDOM() LIMIT #{n}")
    end)
  end

  @impl true
  def slice(df, %Range{} = range) do
    compute(df) |> ExplorerDuckDB.DataFrame.slice(range)
  end

  def slice(df, indices) when is_list(indices) do
    compute(df) |> ExplorerDuckDB.DataFrame.slice(indices)
  end

  @impl true
  def slice(df, offset, length) do
    actual_offset = if offset < 0, do: max(n_rows(df) + offset, 0), else: offset
    push(df, {:limit, length, actual_offset})
  end

  @impl true
  def drop_nil(df, columns), do: push(df, {:drop_nil, columns})

  @impl true
  def pivot_wider(df, _id_columns, names_from, values_from, names_prefix) do
    query_with_eager(df, fn db, table, _eager ->
      values_cols = Enum.map_join(values_from, ", ", &"\"#{&1}\"")
      sql = "PIVOT #{table} ON \"#{names_from}\" USING FIRST(#{values_cols})"
      sql = if names_prefix != "", do: sql <> " GROUP BY ALL", else: sql
      query_sql(db, sql)
    end)
  end

  @impl true
  def pivot_longer(df, _out_df, columns_to_pivot, _columns_to_keep, names_to, values_to) do
    query_with_eager(df, fn db, table, _eager ->
      pivot_cols = Enum.map_join(columns_to_pivot, ", ", &"\"#{&1}\"")
      query_sql(db, "UNPIVOT #{table} ON #{pivot_cols} INTO NAME \"#{names_to}\" VALUE \"#{values_to}\"")
    end)
  end

  @impl true
  def transpose(df, _out_df, _keep_names_as, _new_column_names) do
    # DuckDB doesn't have a native TRANSPOSE; implement via Elixir
    eager = compute_to_eager(df)
    rows = ExplorerDuckDB.DataFrame.to_rows(eager, false)
    names = Explorer.DataFrame.names(eager)

    n_rows = length(rows)
    new_cols =
      for i <- 0..(n_rows - 1) do
        {"column_#{i}", Enum.map(names, fn name -> Enum.at(rows, i) |> Map.get(name) end)}
      end

    Explorer.DataFrame.new(new_cols)
  end

  @impl true
  def put(df, out_df, column_name, series) do
    expr = SQLBuilder.to_sql(series)
    push(df, {:mutate, out_df.names, [{column_name, expr}]})
  end

  @impl true
  def nil_count(df) do
    push(df, {:nil_count, df.names}) |> compute()
  end

  @impl true
  def explode(df, _out_df, columns) do
    query_with_eager(df, fn db, table, eager ->
      unnest_cols = Enum.map_join(columns, ", ", &"UNNEST(\"#{&1}\") AS \"#{&1}\"")
      other_cols = eager.names |> Enum.reject(&(&1 in columns)) |> Enum.map_join(", ", &"\"#{&1}\"")
      select = if other_cols == "", do: unnest_cols, else: "#{other_cols}, #{unnest_cols}"
      query_sql(db, "SELECT #{select} FROM #{table}")
    end)
  end

  @impl true
  def unnest(df, _out_df, columns) do
    query_with_eager(df, fn db, table, eager ->
      other_cols = eager.names |> Enum.reject(&(&1 in columns)) |> Enum.map_join(", ", &"\"#{&1}\"")
      struct_expansions = Enum.map_join(columns, ", ", &"\"#{&1}\".*")
      select = if other_cols == "", do: struct_expansions, else: "#{other_cols}, #{struct_expansions}"
      query_sql(db, "SELECT #{select} FROM #{table}")
    end)
  end

  @impl true
  def correlation(df, _out_df, _method) do
    query_with_eager(df, fn db, table, eager ->
      numeric_cols = for {name, dtype} <- eager.dtypes, match?({:s, _}, dtype) or match?({:u, _}, dtype) or match?({:f, _}, dtype), do: name

      corr_parts =
        for col1 <- numeric_cols, col2 <- numeric_cols do
          "CORR(\"#{col1}\", \"#{col2}\") AS \"#{col1}_#{col2}\""
        end

      query_sql(db, "SELECT #{Enum.join(corr_parts, ", ")} FROM #{table}")
    end)
  end

  @impl true
  def covariance(df, _out_df, _ddof) do
    query_with_eager(df, fn db, table, eager ->
      numeric_cols = for {name, dtype} <- eager.dtypes, match?({:s, _}, dtype) or match?({:u, _}, dtype) or match?({:f, _}, dtype), do: name

      cov_parts =
        for col1 <- numeric_cols, col2 <- numeric_cols do
          "COVAR_SAMP(\"#{col1}\", \"#{col2}\") AS \"#{col1}_#{col2}\""
        end

      query_sql(db, "SELECT #{Enum.join(cov_parts, ", ")} FROM #{table}")
    end)
  end

  # ============================================================
  # Multi-table verbs
  # ============================================================

  @impl true
  def join([left_df, right_df], out_df, on, how) do
    db = Shared.get_db()
    right_eager = ensure_eager(right_df)
    right_table = ExplorerDuckDB.DataFrame.register_df(db, right_eager)

    # Build select columns with side prefixes
    # out_df may have renamed columns (e.g., "val_right" for duplicate "val")
    # We need to map back to the original column names in each table
    left_names_set = MapSet.new(left_df.names)
    right_names_set = MapSet.new(right_df.names)
    _on_left = MapSet.new(Enum.map(on, &elem(&1, 0)))

    select_cols =
      Enum.map(out_df.names, fn out_name ->
        cond do
          # If the name exists in the left table, use it from left
          MapSet.member?(left_names_set, out_name) ->
            {"l", out_name, out_name}

          # If the name exists in the right table, use it from right
          MapSet.member?(right_names_set, out_name) ->
            {"r", out_name, out_name}

          # Otherwise it's a renamed column -- try to find the original
          # Explorer typically appends "_right" suffix
          String.ends_with?(out_name, "_right") ->
            original = String.replace_suffix(out_name, "_right", "")

            if MapSet.member?(right_names_set, original) do
              {"r", original, out_name}
            else
              {"r", out_name, out_name}
            end

          true ->
            {"r", out_name, out_name}
        end
      end)

    # If left is lazy, push join op; if eager, convert to lazy first
    left_lazy =
      case left_df.data do
        %__MODULE__{} -> left_df
        %ExplorerDuckDB.DataFrame{} -> from_eager(left_df)
      end

    push(left_lazy, {:join, right_table, how, on, select_cols})
  end

  @impl true
  def concat_columns(dfs, _out_df) do
    db = Shared.get_db()
    eager_dfs = Enum.map(dfs, &ensure_eager/1)

    ExplorerDuckDB.DataFrame.with_temp_tables(db, eager_dfs, fn tables ->
      [{first_table, first_df} | rest] = Enum.zip(tables, eager_dfs)

      first_cols = Enum.map_join(first_df.names, ", ", &"t0.\"#{&1}\"")

      rest_parts =
        rest
        |> Enum.with_index(1)
        |> Enum.map(fn {{table, df}, idx} ->
          cols = Enum.map_join(df.names, ", ", &"t#{idx}.\"#{&1}\"")
          join = "JOIN (SELECT *, ROW_NUMBER() OVER () AS __rn FROM #{table}) t#{idx} ON t0.__rn = t#{idx}.__rn"
          {cols, join}
        end)

      all_cols = [first_cols | Enum.map(rest_parts, &elem(&1, 0))] |> Enum.join(", ")
      all_joins = Enum.map_join(rest_parts, " ", &elem(&1, 1))
      sql = "SELECT #{all_cols} FROM (SELECT *, ROW_NUMBER() OVER () AS __rn FROM #{first_table}) t0 #{all_joins}"
      query_sql(db, sql)
    end)
  end

  @impl true
  def concat_rows(dfs, out_df) do
    db = Shared.get_db()
    eager_dfs = Enum.map(dfs, &ensure_eager/1)

    ExplorerDuckDB.DataFrame.with_temp_tables(db, eager_dfs, fn sources ->
      [first_source | rest_sources] = sources

      lazy_data = %__MODULE__{
        db: db,
        source: first_source,
        operations: [{:union_all, rest_sources, out_df.names}]
      }

      # Don't cleanup -- compute will handle source, but rest_sources are referenced in the SQL
      # We need to compute before cleanup happens
      result = %{out_df | data: lazy_data} |> compute_to_eager()
      result
    end)
  end

  # ============================================================
  # Groups
  # ============================================================

  @impl true
  def summarise_with(df, _out_df, aggregations) do
    group_cols = df.data.groups

    agg_exprs =
      Enum.map(aggregations, fn {name, lazy_series} ->
        {name, SQLBuilder.to_sql(lazy_series)}
      end)

    push(df, {:summarise, group_cols, agg_exprs})
    |> compute()
  end

  # ============================================================
  # SQL
  # ============================================================

  @impl true
  def sql(df, sql_string, table_name) do
    push(df, {:raw_sql, sql_string, table_name}) |> compute()
  end

  # ============================================================
  # Private helpers
  # ============================================================

  # Compute to eager, register as temp table, run a function, cleanup.
  defp query_with_eager(df, fun) do
    eager = compute_to_eager(df)
    db = Shared.get_db()
    ExplorerDuckDB.DataFrame.with_temp_table(db, eager, fn table ->
      fun.(db, table, eager)
    end)
  end

  defp query_sql(db, sql) do
    case Native.df_query(db, sql) do
      {:ok, ref} -> Shared.create_dataframe!(ref)
      ref when is_reference(ref) -> Shared.create_dataframe!(ref)
      {:error, error} -> raise RuntimeError, to_string(error)
    end
  end

  defp ensure_eager(df) do
    case df.data do
      %__MODULE__{} -> compute_to_eager(df)
      %ExplorerDuckDB.DataFrame{} -> df
    end
  end
end
