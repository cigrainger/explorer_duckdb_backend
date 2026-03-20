defmodule ExplorerDuckDB.Series do
  @moduledoc false

  @behaviour Explorer.Backend.Series

  alias ExplorerDuckDB.Native
  alias ExplorerDuckDB.Shared
  alias Explorer.Series

  import Kernel, except: [is_nil: 1]

  defstruct resource: nil, pending_sql: nil, source_table: nil, source_db: nil

  @type t :: %__MODULE__{
          resource: reference() | nil,
          pending_sql: String.t() | nil,
          source_table: String.t() | nil,
          source_db: reference() | nil
        }

  # Used by DataFrame.from_series to create a series without wrapping in Explorer.Series
  def from_list_for_df(list, dtype, name) do
    case Shared.from_list(list, dtype, name) do
      {:ok, series_ref} -> Shared.create_series(series_ref)
      {:error, error} -> raise RuntimeError, to_string(error)
      series_ref when is_reference(series_ref) -> Shared.create_series(series_ref)
    end
  end

  # ============================================================
  # Conversion
  # ============================================================

  @impl true
  def from_list(list, dtype) do
    series =
      case Shared.from_list(list, dtype) do
        {:ok, series_ref} -> Shared.create_series(series_ref)
        {:error, error} -> raise RuntimeError, to_string(error)
        series_ref when is_reference(series_ref) -> Shared.create_series(series_ref)
      end

    # Override dtype for types that DuckDB stores differently
    case dtype do
      :category -> %{series | dtype: :category}
      :date -> %{series | dtype: :date}
      :time -> %{series | dtype: :time}
      {:naive_datetime, _} -> %{series | dtype: dtype}
      {:datetime, _, _} -> %{series | dtype: dtype}
      _ -> series
    end
  end

  @impl true
  def from_binary(binary, dtype) do
    list =
      case dtype do
        {:s, 8} -> for <<v::8-signed-native <- binary>>, do: v
        {:s, 16} -> for <<v::16-signed-native <- binary>>, do: v
        {:s, 32} -> for <<v::32-signed-native <- binary>>, do: v
        {:s, 64} -> for <<v::64-signed-native <- binary>>, do: v
        {:u, 8} -> for <<v::8-unsigned-native <- binary>>, do: v
        {:u, 16} -> for <<v::16-unsigned-native <- binary>>, do: v
        {:u, 32} -> for <<v::32-unsigned-native <- binary>>, do: v
        {:u, 64} -> for <<v::64-unsigned-native <- binary>>, do: v
        {:f, 32} -> for <<v::32-float-native <- binary>>, do: v
        {:f, 64} -> for <<v::64-float-native <- binary>>, do: v
        :boolean -> for <<v::8-unsigned-native <- binary>>, do: v != 0
        _ -> raise "from_binary not supported for dtype #{Kernel.inspect(dtype)}"
      end

    from_list(list, dtype)
  end

  @impl true
  def to_list(series) do
    series = ensure_materialized(series)

    case Native.s_to_list(series.data.resource) do
      {:ok, list} -> list
      list when is_list(list) -> list
      {:error, error} -> raise RuntimeError, to_string(error)
    end
  end

  @impl true
  def to_iovec(series) do
    list = to_list(series)

    binary =
      case series.dtype do
        {:s, 8} -> for v <- list, into: <<>>, do: <<v::8-signed-native>>
        {:s, 16} -> for v <- list, into: <<>>, do: <<v::16-signed-native>>
        {:s, 32} -> for v <- list, into: <<>>, do: <<v::32-signed-native>>
        {:s, 64} -> for v <- list, into: <<>>, do: <<v::64-signed-native>>
        {:u, 8} -> for v <- list, into: <<>>, do: <<v::8-unsigned-native>>
        {:u, 16} -> for v <- list, into: <<>>, do: <<v::16-unsigned-native>>
        {:u, 32} -> for v <- list, into: <<>>, do: <<v::32-unsigned-native>>
        {:u, 64} -> for v <- list, into: <<>>, do: <<v::64-unsigned-native>>
        {:f, 32} -> for v <- list, into: <<>>, do: <<v::32-float-native>>
        {:f, 64} -> for v <- list, into: <<>>, do: <<v::64-float-native>>
        _ -> raise "to_iovec not supported for dtype #{Kernel.inspect(series.dtype)}"
      end

    [binary]
  end

  @impl true
  def cast(series, dtype) do
    duckdb_type = ExplorerDuckDB.DataFrame.explorer_dtype_to_duckdb_sql(dtype)
    result = sql_transform(series, "CAST(\"value\" AS #{duckdb_type})")
    # Update dtype since cast changes it
    %{result | dtype: dtype}
  end

  @impl true
  def categorise(series, categories_series) do
    # Map integer indices to category string values
    cat_list = Series.to_list(categories_series)
    values = to_list(series)

    result =
      Enum.map(values, fn
        nil -> nil
        idx when is_integer(idx) and idx >= 0 and idx < length(cat_list) -> Enum.at(cat_list, idx)
        _ -> nil
      end)

    # Return as category dtype
    s = from_list(result, :string)
    %{s | dtype: :category}
  end

  @impl true
  def strptime(series, format_string) do
    escaped = String.replace(format_string, "'", "''")
    sql_transform(series, "strptime(\"value\", '#{escaped}')")
  end

  @impl true
  def strftime(series, format_string) do
    escaped = String.replace(format_string, "'", "''")
    sql_transform(series, "strftime(\"value\", '#{escaped}')")
  end

  # ============================================================
  # Introspection
  # ============================================================

  @impl true
  def size(series) do
    series = ensure_materialized(series)

    case Native.s_size(series.data.resource) do
      {:ok, n} -> n
      n when is_integer(n) -> n
    end
  end

  @impl true
  def inspect(series, opts) do
    series = ensure_materialized(series)
    n_rows = size(series)
    Explorer.Backend.Series.inspect(series, "DuckDB", n_rows, opts)
  end

  @impl true
  def categories(series) do
    # Return the unique category values as a string series
    db = Shared.get_db()
    table = create_temp_table(db, series)

    try do
      query_series(db, "SELECT DISTINCT \"value\" FROM #{table} WHERE \"value\" IS NOT NULL ORDER BY \"value\"")
    after
      cleanup(db, table)
    end
  end

  @impl true
  def owner_reference(_s), do: nil
  @impl true
  def owner_import(_term), do: {:error, RuntimeError.exception("owner_import not supported")}
  @impl true
  def owner_export(_s), do: {:error, RuntimeError.exception("owner_export not supported")}

  # ============================================================
  # Slice and dice
  # ============================================================

  @impl true
  def head(series, n), do: slice(series, 0, n)

  @impl true
  def tail(series, n) do
    total = size(series)
    offset = max(total - n, 0)
    slice(series, offset, n)
  end

  @impl true
  def sample(series, n_or_frac, replacement, _shuffle, seed) do
    n =
      if is_float(n_or_frac) do
        trunc(size(series) * n_or_frac)
      else
        n_or_frac
      end

    seed_sql = if seed, do: "SETSEED(#{seed / 2_147_483_647}); ", else: ""

    if replacement do
      # With replacement: multiple random picks
      db = Shared.get_db()
      table = create_temp_table(db, series)

      sql =
        "#{seed_sql}SELECT \"value\" FROM #{table} ORDER BY RANDOM() LIMIT #{n}"

      result = query_series(db, sql)
      cleanup(db, table)
      result
    else
      db = Shared.get_db()
      table = create_temp_table(db, series)
      sql = "#{seed_sql}SELECT \"value\" FROM #{table} ORDER BY RANDOM() LIMIT #{n}"
      result = query_series(db, sql)
      cleanup(db, table)
      result
    end
  end

  @impl true
  def at(series, idx) do
    series = ensure_materialized(series)

    case Native.s_at(series.data.resource, idx) do
      {:ok, val} -> val
      val -> val
    end
  end

  @impl true
  def at_every(series, every_n) do
    series = ensure_materialized(series)
    db = Shared.get_db()
    table = create_temp_table(db, series)

    try do
      sql = "SELECT \"value\" FROM (SELECT \"value\", (ROW_NUMBER() OVER (ORDER BY __rowid) - 1) AS __idx FROM #{table}) WHERE __idx % #{every_n} = 0"
      query_series(db, sql)
    after
      cleanup(db, table)
    end
  end

  @impl true
  def mask(series, %Series{} = mask_series) do
    series = ensure_materialized(series)
    mask_series = ensure_materialized(mask_series)
    db = Shared.get_db()
    t1 = create_temp_table(db, series)
    t2 = create_temp_table(db, mask_series)

    try do
      sql = "SELECT a.\"value\" FROM #{t1} a JOIN #{t2} b ON a.__rowid = b.__rowid WHERE b.\"value\" = true ORDER BY a.__rowid"
      query_series(db, sql)
    after
      cleanup(db, t1)
      cleanup(db, t2)
    end
  end

  @impl true
  def slice(series, indices) when is_list(indices) do
    series = ensure_materialized(series)
    db = Shared.get_db()
    table = create_temp_table(db, series)

    try do
      # Handle negative indices by converting to positive
      n = size(series)
      positive_indices = Enum.map(indices, fn i -> if i < 0, do: n + i, else: i end)
      idx_list = Enum.join(positive_indices, ", ")

      sql = "SELECT \"value\" FROM (SELECT \"value\", ROW_NUMBER() OVER (ORDER BY __rowid) - 1 AS __idx FROM #{table}) WHERE __idx IN (#{idx_list}) ORDER BY __idx"
      query_series(db, sql)
    after
      cleanup(db, table)
    end
  end

  def slice(series, %Range{first: first, last: last, step: step}) do
    series = ensure_materialized(series)
    n = size(series)
    first = if first < 0, do: n + first, else: first
    last = if last < 0, do: n + last, else: last
    indices = Enum.to_list(first..last//step)
    slice(series, indices)
  end

  @impl true
  def slice(series, offset, length) do
    series = ensure_materialized(series)

    case Native.s_slice(series.data.resource, offset, length) do
      {:ok, ref} -> Shared.create_series(ref)
      ref when is_reference(ref) -> Shared.create_series(ref)
      {:error, error} -> raise RuntimeError, to_string(error)
    end
  end

  @impl true
  def format(series_list) do
    db = Shared.get_db()
    materialized = Enum.map(series_list, &ensure_materialized/1)
    tables = Enum.with_index(materialized, fn s, i -> {create_temp_table(db, s), "t#{i}"} end)

    try do
      concat_expr = Enum.map_join(tables, " || ", fn {_, alias_name} -> "#{alias_name}.\"value\"::VARCHAR" end)
      from_clause = tables |> Enum.with_index() |> Enum.map_join(" ", fn {{table, alias_name}, idx} ->
        if idx == 0, do: "#{table} #{alias_name}", else: "JOIN #{table} #{alias_name} ON t0.__rowid = #{alias_name}.__rowid"
      end)

      sql = "SELECT #{concat_expr} AS \"value\" FROM #{from_clause} ORDER BY t0.__rowid"
      query_series(db, sql)
    after
      Enum.each(tables, fn {table, _} -> cleanup(db, table) end)
    end
  end

  @impl true
  def concat(series_list) do
    db = Shared.get_db()
    materialized = Enum.map(series_list, &ensure_materialized/1)
    tables = Enum.map(materialized, fn s -> create_temp_table(db, s) end)

    try do
      union_sql = Enum.map_join(tables, " UNION ALL ", fn t -> "(SELECT \"value\" FROM #{t} ORDER BY __rowid)" end)
      query_series(db, union_sql)
    after
      Enum.each(tables, fn t -> cleanup(db, t) end)
    end
  end

  @impl true
  def coalesce(s1, s2) do
    s1 = ensure_materialized(s1)
    s2 = ensure_materialized(s2)
    db = Shared.get_db()
    t1 = create_temp_table(db, s1)
    t2 = create_temp_table(db, s2)

    try do
      sql = "SELECT COALESCE(a.\"value\", b.\"value\") AS \"value\" FROM #{t1} a JOIN #{t2} b ON a.__rowid = b.__rowid ORDER BY a.__rowid"
      query_series(db, sql)
    after
      cleanup(db, t1)
      cleanup(db, t2)
    end
  end

  @impl true
  def first(series), do: at(series, 0)

  @impl true
  def last(series) do
    n = size(series)
    if n > 0, do: at(series, n - 1), else: nil
  end

  @impl true
  def select(pred, on_true, on_false) do
    pred = ensure_materialized(pred)
    on_true = ensure_materialized(on_true)
    on_false = ensure_materialized(on_false)
    db = Shared.get_db()
    tp = create_temp_table(db, pred)
    tt = create_temp_table(db, on_true)
    tf = create_temp_table(db, on_false)

    try do
      sql = "SELECT CASE WHEN p.\"value\" THEN t.\"value\" ELSE f.\"value\" END AS \"value\" " <>
        "FROM #{tp} p JOIN #{tt} t ON p.__rowid = t.__rowid JOIN #{tf} f ON p.__rowid = f.__rowid ORDER BY p.__rowid"
      query_series(db, sql)
    after
      cleanup(db, tp)
      cleanup(db, tt)
      cleanup(db, tf)
    end
  end

  @impl true
  def shift(series, offset, _default) do
    series = ensure_materialized(series)
    db = Shared.get_db()
    table = create_temp_table(db, series)

    try do
      if offset >= 0 do
        sql = "SELECT LAG(\"value\", #{offset}) OVER (ORDER BY __rowid) AS \"value\" FROM #{table} ORDER BY __rowid"
        query_series(db, sql)
      else
        sql = "SELECT LEAD(\"value\", #{-offset}) OVER (ORDER BY __rowid) AS \"value\" FROM #{table} ORDER BY __rowid"
        query_series(db, sql)
      end
    after
      cleanup(db, table)
    end
  end

  @impl true
  def rank(series, method, descending, _seed) do
    rank_fn =
      case method do
        :ordinal -> "ROW_NUMBER"
        :dense -> "DENSE_RANK"
        :min -> "RANK"
        :max -> "RANK"
        _ -> "RANK"
      end

    dir = if descending, do: "DESC", else: "ASC"

    db = Shared.get_db()
    table = create_temp_table(db, series)
    sql = "SELECT #{rank_fn}() OVER (ORDER BY \"value\" #{dir}) AS \"value\" FROM #{table}"
    result = query_series(db, sql)
    cleanup(db, table)
    result
  end

  # ============================================================
  # Aggregation
  # ============================================================

  @impl true
  def count(series), do: size(series)

  @impl true
  def sum(series), do: scalar_agg(series, "SUM")

  @impl true
  def min(series), do: scalar_agg(series, "MIN")

  @impl true
  def max(series), do: scalar_agg(series, "MAX")

  @impl true
  def argmin(series) do
    db = Shared.get_db()
    table = create_temp_table(db, series)
    sql = "SELECT __rn FROM (SELECT ROW_NUMBER() OVER () - 1 AS __rn, \"value\" FROM #{table}) ORDER BY \"value\" ASC NULLS LAST LIMIT 1"
    result = scalar_query(db, sql)
    cleanup(db, table)
    result
  end

  @impl true
  def argmax(series) do
    db = Shared.get_db()
    table = create_temp_table(db, series)
    sql = "SELECT __rn FROM (SELECT ROW_NUMBER() OVER () - 1 AS __rn, \"value\" FROM #{table}) ORDER BY \"value\" DESC NULLS LAST LIMIT 1"
    result = scalar_query(db, sql)
    cleanup(db, table)
    result
  end

  @impl true
  def mean(series), do: scalar_agg(series, "AVG")

  @impl true
  def median(series), do: scalar_agg(series, "MEDIAN")

  @impl true
  def mode(series) do
    db = Shared.get_db()
    table = create_temp_table(db, series)
    sql = "SELECT MODE(\"value\") AS \"value\" FROM #{table}"
    result = query_series(db, sql)
    cleanup(db, table)
    result
  end

  @impl true
  def variance(series, _ddof), do: scalar_agg(series, "VARIANCE")

  @impl true
  def standard_deviation(series, _ddof), do: scalar_agg(series, "STDDEV")

  @impl true
  def quantile(series, q) do
    db = Shared.get_db()
    table = create_temp_table(db, series)
    sql = "SELECT QUANTILE_CONT(\"value\", #{q}) FROM #{table}"
    result = scalar_query(db, sql)
    cleanup(db, table)
    result
  end

  @impl true
  def nil_count(series) do
    db = Shared.get_db()
    table = create_temp_table(db, series)
    sql = "SELECT COUNT(*) FILTER (WHERE \"value\" IS NULL) FROM #{table}"
    result = scalar_query(db, sql)
    cleanup(db, table)
    result || 0
  end

  @impl true
  def product(series), do: scalar_agg(series, "PRODUCT")

  @impl true
  def skew(series, _bias?) do
    db = Shared.get_db()
    table = create_temp_table(db, series)
    sql = "SELECT SKEWNESS(\"value\") FROM #{table}"
    result = scalar_query(db, sql)
    cleanup(db, table)
    result
  end

  @impl true
  def correlation(s1, s2, _method) do
    db = Shared.get_db()
    t1 = create_temp_table(db, s1)
    t2 = create_temp_table(db, s2)
    sql = "SELECT CORR(a.\"value\", b.\"value\") FROM " <>
      "#{t1} a JOIN #{t2} b ON a.__rowid = b.__rowid"
    result = scalar_query(db, sql)
    cleanup(db, t1)
    cleanup(db, t2)
    result
  end

  @impl true
  def covariance(s1, s2, _ddof) do
    db = Shared.get_db()
    t1 = create_temp_table(db, s1)
    t2 = create_temp_table(db, s2)
    sql = "SELECT COVAR_SAMP(a.\"value\", b.\"value\") FROM " <>
      "#{t1} a JOIN #{t2} b ON a.__rowid = b.__rowid"
    result = scalar_query(db, sql)
    cleanup(db, t1)
    cleanup(db, t2)
    result
  end

  @impl true
  def all?(series) do
    series = ensure_materialized(series)
    db = Shared.get_db()
    table = create_temp_table(db, series)

    try do
      scalar_query(db, "SELECT BOOL_AND(\"value\") FROM #{table}")
    after
      cleanup(db, table)
    end
  end

  @impl true
  def any?(series) do
    series = ensure_materialized(series)
    db = Shared.get_db()
    table = create_temp_table(db, series)

    try do
      scalar_query(db, "SELECT BOOL_OR(\"value\") FROM #{table}")
    after
      cleanup(db, table)
    end
  end

  @impl true
  def row_index(series) do
    series = ensure_materialized(series)
    db = Shared.get_db()
    table = create_temp_table(db, series)

    try do
      query_series(db, "SELECT (ROW_NUMBER() OVER (ORDER BY __rowid) - 1)::UINTEGER AS \"value\" FROM #{table}")
    after
      cleanup(db, table)
    end
  end

  # ============================================================
  # Cumulative
  # ============================================================

  @impl true
  def cumulative_max(series, reverse?) do
    cumulative_window(series, "MAX", reverse?)
  end

  @impl true
  def cumulative_min(series, reverse?) do
    cumulative_window(series, "MIN", reverse?)
  end

  @impl true
  def cumulative_sum(series, reverse?) do
    cumulative_window(series, "SUM", reverse?)
  end

  @impl true
  def cumulative_count(series, reverse?) do
    cumulative_window(series, "COUNT", reverse?)
  end

  @impl true
  def cumulative_product(series, reverse?) do
    cumulative_window(series, "PRODUCT", reverse?)
  end

  # ============================================================
  # Local minima/maxima
  # ============================================================

  @impl true
  def peaks(series, kind) do
    series = ensure_materialized(series)
    db = Shared.get_db()
    table = create_temp_table(db, series)

    try do
      comp = if kind == :max, do: ">=", else: "<="

      sql = """
        SELECT CASE
          WHEN \"value\" #{comp} COALESCE(LAG(\"value\") OVER (ORDER BY __rowid), \"value\")
           AND \"value\" #{comp} COALESCE(LEAD(\"value\") OVER (ORDER BY __rowid), \"value\")
          THEN true ELSE false END AS "value"
        FROM #{table} ORDER BY __rowid
      """

      query_series(db, sql)
    after
      cleanup(db, table)
    end
  end

  # ============================================================
  # Arithmetic
  # ============================================================

  @impl true
  def add(_out_dtype, left, right), do: binary_series_op(left, right, "+")
  @impl true
  def subtract(_out_dtype, left, right), do: binary_series_op(left, right, "-")
  @impl true
  def multiply(_out_dtype, left, right), do: binary_series_op(left, right, "*")
  @impl true
  def divide(_out_dtype, left, right), do: binary_series_op(left, right, "/")
  @impl true
  def quotient(left, right), do: binary_series_op(left, right, "//")
  @impl true
  def remainder(left, right), do: binary_series_op(left, right, "%")
  @impl true
  def pow(_out_dtype, base, exponent), do: binary_series_op(base, exponent, "**")

  @impl true
  def log(s), do: sql_transform(s, "LN(\"value\")")
  @impl true
  def log(s, base), do: sql_transform(s, "LOG(\"value\") / LOG(#{base})")
  @impl true
  def exp(s), do: sql_transform(s, "EXP(\"value\")")
  @impl true
  def abs(s), do: sql_transform(s, "ABS(\"value\")")

  @impl true
  def clip(s, min_val, max_val) do
    sql_transform(s, "GREATEST(LEAST(\"value\", #{max_val}), #{min_val})")
  end

  # ============================================================
  # Trigonometry
  # ============================================================

  @impl true
  def acos(s), do: sql_transform(s, "ACOS(\"value\")")
  @impl true
  def asin(s), do: sql_transform(s, "ASIN(\"value\")")
  @impl true
  def atan(s), do: sql_transform(s, "ATAN(\"value\")")
  @impl true
  def cos(s), do: sql_transform(s, "COS(\"value\")")
  @impl true
  def sin(s), do: sql_transform(s, "SIN(\"value\")")
  @impl true
  def tan(s), do: sql_transform(s, "TAN(\"value\")")
  @impl true
  def degrees(s), do: sql_transform(s, "DEGREES(\"value\")")
  @impl true
  def radians(s), do: sql_transform(s, "RADIANS(\"value\")")

  # ============================================================
  # Comparisons
  # ============================================================

  @impl true
  def equal(left, right), do: binary_series_op(left, right, "=")
  @impl true
  def not_equal(left, right), do: binary_series_op(left, right, "!=")
  @impl true
  def greater(left, right), do: binary_series_op(left, right, ">")
  @impl true
  def greater_equal(left, right), do: binary_series_op(left, right, ">=")
  @impl true
  def less(left, right), do: binary_series_op(left, right, "<")
  @impl true
  def less_equal(left, right), do: binary_series_op(left, right, "<=")

  @impl true
  def all_equal(left, right) do
    neq = not_equal(left, right)
    not any?(neq)
  end

  @impl true
  def binary_and(left, right), do: binary_series_op(left, right, "AND")
  @impl true
  def binary_or(left, right), do: binary_series_op(left, right, "OR")

  @impl true
  def binary_in(s, other) do
    other_list = Series.to_list(other)
    values_sql = Enum.map_join(other_list, ", ", &value_to_sql/1)
    sql_transform(s, "\"value\" IN (#{values_sql})")
  end

  # ============================================================
  # Float predicates
  # ============================================================

  @impl true
  def is_finite(s), do: sql_transform(s, "isfinite(\"value\")")
  @impl true
  def is_infinite(s), do: sql_transform(s, "isinf(\"value\")")
  @impl true
  def is_nan(s), do: sql_transform(s, "isnan(\"value\")")

  # ============================================================
  # Float rounding
  # ============================================================

  @impl true
  def round(s, decimals), do: sql_transform(s, "ROUND(\"value\", #{decimals})")
  @impl true
  def floor(s), do: sql_transform(s, "FLOOR(\"value\")")
  @impl true
  def ceil(s), do: sql_transform(s, "CEIL(\"value\")")

  # ============================================================
  # Sort
  # ============================================================

  @impl true
  def sort(series, descending?, _maintain_order?, _multithreaded?, nulls_last?) do
    series = ensure_materialized(series)
    db = Shared.get_db()

    case Native.s_sort(db, series.data.resource, descending?, nulls_last?) do
      {:ok, ref} -> Shared.create_series(ref)
      ref when is_reference(ref) -> Shared.create_series(ref)
      {:error, error} -> raise RuntimeError, to_string(error)
    end
  end

  @impl true
  def argsort(series, descending?, _maintain?, _multi?, nulls_last?) do
    # Implement in Elixir for correctness -- DuckDB temp tables don't guarantee insertion order
    list = to_list(series)
    dir = if descending?, do: :desc, else: :asc

    indices =
      list
      |> Enum.with_index()
      |> Enum.sort_by(
        fn {val, _idx} -> val end,
        fn
          nil, nil -> true
          nil, _ -> not nulls_last?
          _, nil -> nulls_last?
          a, b -> if dir == :asc, do: a <= b, else: a >= b
        end
      )
      |> Enum.map(&elem(&1, 1))

    from_list(indices, {:u, 32})
  end

  @impl true
  def reverse(series) do
    series = ensure_materialized(series)

    case Native.s_reverse(series.data.resource) do
      {:ok, ref} -> Shared.create_series(ref)
      ref when is_reference(ref) -> Shared.create_series(ref)
      {:error, error} -> raise RuntimeError, to_string(error)
    end
  end

  # ============================================================
  # Distinct
  # ============================================================

  @impl true
  def distinct(s) do
    db = Shared.get_db()
    table = create_temp_table(db, s)
    sql = "SELECT DISTINCT \"value\" FROM #{table} ORDER BY \"value\""
    result = query_series(db, sql)
    cleanup(db, table)
    result
  end

  @impl true
  def unordered_distinct(s) do
    db = Shared.get_db()
    table = create_temp_table(db, s)
    sql = "SELECT DISTINCT \"value\" FROM #{table}"
    result = query_series(db, sql)
    cleanup(db, table)
    result
  end

  @impl true
  def n_distinct(s) do
    db = Shared.get_db()
    table = create_temp_table(db, s)
    sql = "SELECT COUNT(DISTINCT \"value\") FROM #{table}"
    result = scalar_query(db, sql)
    cleanup(db, table)
    result
  end

  @impl true
  def frequencies(s) do
    db = Shared.get_db()
    table = create_temp_table(db, s)
    sql = "SELECT \"value\", COUNT(*) AS counts FROM #{table} GROUP BY \"value\" ORDER BY counts DESC"

    result =
      case Native.df_query(db, sql) do
        {:ok, ref} -> Shared.create_dataframe!(ref)
        ref when is_reference(ref) -> Shared.create_dataframe!(ref)
      end

    cleanup(db, table)
    result
  end

  # ============================================================
  # Categorisation
  # ============================================================

  @impl true
  def cut(s, bins, labels, break_point_label, category_label, _left_close, _include_breaks) do
    db = Shared.get_db()
    table = create_temp_table(db, s)
    bp_label = break_point_label || "break_point"
    cat_label = category_label || "category"

    try do
      # Build CASE expression for binning
      all_bins = [-1.0e308 | bins] ++ [1.0e308]
      n_bins = length(all_bins) - 1

      case_parts =
        for i <- 0..(n_bins - 1) do
          lo = Enum.at(all_bins, i)
          hi = Enum.at(all_bins, i + 1)
          label =
            if labels do
              Enum.at(labels, i, "unknown")
            else
              lo_str = if lo == -1.0e308, do: "-inf", else: "#{lo}"
              hi_str = if hi == 1.0e308, do: "inf", else: "#{hi}"
              "(#{lo_str}, #{hi_str}]"
            end
          escaped = String.replace(label, "'", "''")
          "WHEN \"value\" > #{lo} AND \"value\" <= #{hi} THEN '#{escaped}'"
        end

      case_sql = "CASE #{Enum.join(case_parts, " ")} ELSE NULL END"

      sql = "SELECT \"value\" AS \"#{bp_label}\", #{case_sql} AS \"#{cat_label}\" FROM #{table} ORDER BY __rowid"
      result =
        case Native.df_query(db, sql) do
          {:ok, ref} -> Shared.create_dataframe!(ref)
          ref when is_reference(ref) -> Shared.create_dataframe!(ref)
        end

      result
    after
      cleanup(db, table)
    end
  end

  @impl true
  def qcut(s, quantiles, labels, break_point_label, category_label, _allow_duplicates, left_close, include_breaks) do
    db = Shared.get_db()
    table = create_temp_table(db, s)

    try do
      # Compute quantile values via DuckDB
      quantile_exprs = Enum.map_join(quantiles, ", ", fn q -> "QUANTILE_CONT(\"value\", #{q})" end)
      sql = "SELECT #{quantile_exprs} FROM #{table}"

      bins =
        case Native.df_query(db, sql) do
          {:ok, ref} ->
            case Native.df_to_rows(ref) do
              {:ok, [row]} -> row |> Map.values()
              [row] -> row |> Map.values()
              _ -> []
            end
          ref when is_reference(ref) ->
            case Native.df_to_rows(ref) do
              {:ok, [row]} -> row |> Map.values()
              [row] -> row |> Map.values()
              _ -> []
            end
        end
        |> Enum.reject(&Kernel.is_nil/1)
        |> Enum.sort()
        |> Enum.dedup()

      # Reuse cut with computed bins (need to restore the series from table)
      cleanup(db, table)
      cut(s, bins, labels, break_point_label, category_label, left_close, include_breaks)
    rescue
      e ->
        cleanup(db, table)
        reraise e, __STACKTRACE__
    end
  end

  # ============================================================
  # Window
  # ============================================================

  @impl true
  def window_sum(s, window_size, _weights, min_periods, center) do
    rolling_window(s, "SUM", window_size, min_periods, center)
  end

  @impl true
  def window_min(s, window_size, _weights, min_periods, center) do
    rolling_window(s, "MIN", window_size, min_periods, center)
  end

  @impl true
  def window_max(s, window_size, _weights, min_periods, center) do
    rolling_window(s, "MAX", window_size, min_periods, center)
  end

  @impl true
  def window_mean(s, window_size, _weights, min_periods, center) do
    rolling_window(s, "AVG", window_size, min_periods, center)
  end

  @impl true
  def window_median(s, window_size, _weights, min_periods, center) do
    rolling_window(s, "MEDIAN", window_size, min_periods, center)
  end

  @impl true
  def window_standard_deviation(s, window_size, _weights, min_periods, center) do
    rolling_window(s, "STDDEV_SAMP", window_size, min_periods, center)
  end

  # ============================================================
  # EWM (workaround via Elixir computation)
  # ============================================================

  @impl true
  def ewm_mean(series, alpha, adjust, _min_periods, _ignore_nils) do
    list = to_list(series)
    result = compute_ewm_mean(list, alpha, adjust)
    from_list(result, {:f, 64})
  end

  @impl true
  def ewm_standard_deviation(series, alpha, adjust, bias, min_periods, ignore_nils) do
    var = ewm_variance(series, alpha, adjust, bias, min_periods, ignore_nils)
    sql_transform(var, "SQRT(\"value\")")
  end

  @impl true
  def ewm_variance(series, alpha, adjust, _bias, _min_periods, _ignore_nils) do
    list = to_list(series)
    means = compute_ewm_mean(list, alpha, adjust)

    result =
      list
      |> Enum.with_index()
      |> Enum.map(fn {val, i} ->
        if Kernel.is_nil(val) do
          nil
        else
          mean = Enum.at(means, i)
          if Kernel.is_nil(mean), do: nil, else: (val - mean) * (val - mean)
        end
      end)

    ewm_of_result = compute_ewm_mean(result, alpha, adjust)
    from_list(ewm_of_result, {:f, 64})
  end

  # ============================================================
  # Nulls
  # ============================================================

  @impl true
  def fill_missing_with_strategy(series, strategy) do
    case strategy do
      :forward -> sql_window_fill(series, "LAST_VALUE(\"value\" IGNORE NULLS) OVER (ORDER BY __rowid ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)")
      :backward -> sql_window_fill(series, "FIRST_VALUE(\"value\" IGNORE NULLS) OVER (ORDER BY __rowid ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)")
      :min -> fill_missing_with_value(series, min(series))
      :max -> fill_missing_with_value(series, max(series))
      :mean -> fill_missing_with_value(series, mean(series))
    end
  end

  @impl true
  def fill_missing_with_value(series, value) do
    sql_val = value_to_sql(value)
    sql_transform(series, "COALESCE(\"value\", #{sql_val})")
  end

  @impl true
  def is_nil(series) do
    series = ensure_materialized(series)

    case Native.s_is_nil(series.data.resource) do
      {:ok, ref} -> Shared.create_series(ref)
      ref when is_reference(ref) -> Shared.create_series(ref)
    end
  end

  @impl true
  def is_not_nil(series) do
    series = ensure_materialized(series)

    case Native.s_is_not_nil(series.data.resource) do
      {:ok, ref} -> Shared.create_series(ref)
      ref when is_reference(ref) -> Shared.create_series(ref)
    end
  end

  # ============================================================
  # Escape hatch
  # ============================================================

  @impl true
  def transform(series, fun) do
    list = to_list(series)
    new_list = Enum.map(list, fun)
    from_list(new_list, series.dtype)
  end

  # ============================================================
  # Inversions
  # ============================================================

  @impl true
  def unary_not(series) do
    sql_transform(series, "NOT \"value\"")
  end

  # ============================================================
  # Strings
  # ============================================================

  @impl true
  def contains(s, pattern) do
    escaped = String.replace(pattern, "'", "''")
    sql_transform(s, "contains(\"value\", '#{escaped}')")
  end

  @impl true
  def upcase(s), do: sql_transform(s, "UPPER(\"value\")")
  @impl true
  def downcase(s), do: sql_transform(s, "LOWER(\"value\")")

  @impl true
  def replace(s, pattern, replacement) do
    p = String.replace(pattern, "'", "''")
    r = String.replace(replacement, "'", "''")
    sql_transform(s, "REPLACE(\"value\", '#{p}', '#{r}')")
  end

  @impl true
  def strip(s, nil), do: sql_transform(s, "TRIM(\"value\")")
  def strip(s, chars) do
    c = String.replace(chars, "'", "''")
    sql_transform(s, "TRIM(\"value\", '#{c}')")
  end

  @impl true
  def lstrip(s, nil), do: sql_transform(s, "LTRIM(\"value\")")
  def lstrip(s, chars) do
    c = String.replace(chars, "'", "''")
    sql_transform(s, "LTRIM(\"value\", '#{c}')")
  end

  @impl true
  def rstrip(s, nil), do: sql_transform(s, "RTRIM(\"value\")")
  def rstrip(s, chars) do
    c = String.replace(chars, "'", "''")
    sql_transform(s, "RTRIM(\"value\", '#{c}')")
  end

  @impl true
  def substring(s, offset, nil), do: sql_transform(s, "SUBSTRING(\"value\", #{offset + 1})")
  def substring(s, offset, length), do: sql_transform(s, "SUBSTRING(\"value\", #{offset + 1}, #{length})")

  @impl true
  def split(s, pattern) do
    p = String.replace(pattern, "'", "''")
    sql_transform(s, "STRING_SPLIT(\"value\", '#{p}')")
  end

  @impl true
  def split_into(s, pattern, names) do
    # Split each string and create a struct with named fields
    p = String.replace(pattern, "'", "''")
    parts_exprs =
      names
      |> Enum.with_index()
      |> Enum.map(fn {name, idx} ->
        name_str = to_string(name)
        "STRING_SPLIT(\"value\", '#{p}')[#{idx + 1}] AS \"#{name_str}\""
      end)

    db = Shared.get_db()
    table = create_temp_table(db, s)

    try do
      sql = "SELECT STRUCT_PACK(#{Enum.join(parts_exprs, ", ")}) AS \"value\" FROM #{table}"
      query_series(db, sql)
    after
      cleanup(db, table)
    end
  end

  @impl true
  def json_decode(s, _dtype) do
    sql_transform(s, "json(\"value\")")
  end

  @impl true
  def json_path_match(s, path) do
    p = String.replace(path, "'", "''")
    sql_transform(s, "json_extract_string(\"value\", '#{p}')")
  end

  @impl true
  def count_matches(s, pattern) do
    p = String.replace(pattern, "'", "''")
    sql_transform(s, "(LENGTH(\"value\") - LENGTH(REPLACE(\"value\", '#{p}', ''))) / LENGTH('#{p}')")
  end

  @impl true
  def re_contains(s, pattern) do
    p = String.replace(pattern, "'", "''")
    sql_transform(s, "regexp_matches(\"value\", '#{p}')")
  end

  @impl true
  def re_replace(s, pattern, replacement) do
    p = String.replace(pattern, "'", "''")
    r = String.replace(replacement, "'", "''")
    sql_transform(s, "regexp_replace(\"value\", '#{p}', '#{r}', 'g')")
  end

  @impl true
  def re_count_matches(s, pattern) do
    # DuckDB doesn't have a direct regex count; use regexp_extract_all + length
    p = String.replace(pattern, "'", "''")
    sql_transform(s, "LENGTH(regexp_extract_all(\"value\", '#{p}'))")
  end

  @impl true
  def re_scan(s, pattern) do
    p = String.replace(pattern, "'", "''")
    sql_transform(s, "regexp_extract_all(\"value\", '#{p}')")
  end

  @impl true
  def re_named_captures(s, pattern) do
    # Extract named groups from regex and build a struct
    # DuckDB's regexp_extract with named groups
    p = String.replace(pattern, "'", "''")

    # Parse named groups from the pattern
    group_names =
      Regex.scan(~r/\(\?<([^>]+)>/, pattern)
      |> Enum.map(fn [_, name] -> name end)

    if group_names == [] do
      raise ArgumentError, "pattern must contain named capture groups"
    end

    struct_parts =
      group_names
      |> Enum.with_index(1)
      |> Enum.map(fn {name, idx} ->
        "'#{name}', regexp_extract(\"value\", '#{p}', #{idx})"
      end)

    db = Shared.get_db()
    table = create_temp_table(db, s)

    try do
      sql = "SELECT STRUCT_PACK(#{Enum.join(struct_parts, ", ")}) AS \"value\" FROM #{table}"
      query_series(db, sql)
    after
      cleanup(db, table)
    end
  end

  # ============================================================
  # Date/DateTime
  # ============================================================

  @impl true
  def year(s), do: sql_transform(s, "EXTRACT(YEAR FROM \"value\")")
  @impl true
  def month(s), do: sql_transform(s, "EXTRACT(MONTH FROM \"value\")")
  def day_of_month(s), do: sql_transform(s, "EXTRACT(DAY FROM \"value\")")
  def is_leap_year(s), do: sql_transform(s, "(EXTRACT(YEAR FROM \"value\") % 4 = 0 AND (EXTRACT(YEAR FROM \"value\") % 100 != 0 OR EXTRACT(YEAR FROM \"value\") % 400 = 0))")
  def quarter_of_year(s), do: sql_transform(s, "EXTRACT(QUARTER FROM \"value\")")
  @impl true
  def day_of_year(s), do: sql_transform(s, "EXTRACT(DOY FROM \"value\")")
  def iso_year(s), do: sql_transform(s, "EXTRACT(ISOYEAR FROM \"value\")")
  @impl true
  def week_of_year(s), do: sql_transform(s, "EXTRACT(WEEK FROM \"value\")")
  @impl true
  def day_of_week(s), do: sql_transform(s, "EXTRACT(DOW FROM \"value\")")
  @impl true
  def hour(s), do: sql_transform(s, "EXTRACT(HOUR FROM \"value\")")
  @impl true
  def minute(s), do: sql_transform(s, "EXTRACT(MINUTE FROM \"value\")")
  @impl true
  def second(s), do: sql_transform(s, "EXTRACT(SECOND FROM \"value\")")
  def nanosecond(s), do: sql_transform(s, "EXTRACT(MICROSECOND FROM \"value\") * 1000")

  # ============================================================
  # List
  # ============================================================

  @impl true
  def join(s, separator) do
    sep = String.replace(separator, "'", "''")
    sql_transform(s, "ARRAY_TO_STRING(\"value\", '#{sep}')")
  end

  @impl true
  def lengths(s), do: sql_transform(s, "LENGTH(\"value\")")

  @impl true
  def member?(s, value) do
    sql_val = value_to_sql(value)
    sql_transform(s, "LIST_CONTAINS(\"value\", #{sql_val})")
  end

  # ============================================================
  # Struct
  # ============================================================

  @impl true
  def field(s, name) do
    escaped = String.replace(name, "'", "''")
    sql_transform(s, "\"value\".\"#{escaped}\"")
  end

  # ============================================================
  # Private helpers
  # ============================================================

  defp scalar_agg(series, agg_fn) do
    series = ensure_materialized(series)
    db = Shared.get_db()

    case Native.s_aggregate_scalar(db, series.data.resource, agg_fn) do
      {:ok, val} -> val
      val -> val
    end
  end

  defp scalar_query(db, sql) do
    case Native.df_query(db, sql) do
      {:ok, ref} ->
        case Native.df_to_rows(ref) do
          {:ok, [row]} -> row |> Map.values() |> hd()
          [row] -> row |> Map.values() |> hd()
          _ -> nil
        end

      ref when is_reference(ref) ->
        case Native.df_to_rows(ref) do
          {:ok, [row]} -> row |> Map.values() |> hd()
          [row] -> row |> Map.values() |> hd()
          _ -> nil
        end
    end
  end

  defp sql_transform(%Series{} = series, sql_expr) do
    backend = series.data

    case backend do
      %__MODULE__{pending_sql: nil, resource: resource} when is_reference(resource) ->
        # First transform on a materialized series -- create temp table and go lazy
        db = Shared.get_db()
        table = create_temp_table(db, series)
        lazy_backend = %__MODULE__{
          resource: nil,
          pending_sql: sql_expr,
          source_table: table,
          source_db: db
        }
        %{series | data: lazy_backend}

      %__MODULE__{pending_sql: prev_sql} when is_binary(prev_sql) ->
        # Chain on existing lazy -- compose the SQL expression
        # Replace "value" references in the new expr with the previous expression
        composed = String.replace(sql_expr, "\"value\"", "(#{prev_sql})")
        lazy_backend = %{backend | pending_sql: composed}
        %{series | data: lazy_backend}
    end
  end

  # Materialize a lazy series by executing its pending SQL
  defp materialize(%Series{data: %__MODULE__{pending_sql: nil}} = series), do: series

  defp materialize(%Series{data: %__MODULE__{pending_sql: sql, source_table: table, source_db: db}}) do
    try do
      result = query_series(db, "SELECT #{sql} AS \"value\" FROM #{table} ORDER BY __rowid")
      result
    after
      cleanup(db, table)
    end
  end

  # Ensure a series is materialized before accessing data
  defp ensure_materialized(%Series{data: %__MODULE__{pending_sql: nil}} = series), do: series

  defp ensure_materialized(%Series{} = series) do
    materialize(series)
  end

  defp query_series(db, sql) do
    case Native.df_query(db, sql) do
      {:ok, ref} ->
        case Native.df_pull(ref, "value") do
          {:ok, s_ref} -> Shared.create_series(s_ref)
          s_ref when is_reference(s_ref) -> Shared.create_series(s_ref)
        end

      ref when is_reference(ref) ->
        case Native.df_pull(ref, "value") do
          {:ok, s_ref} -> Shared.create_series(s_ref)
          s_ref when is_reference(s_ref) -> Shared.create_series(s_ref)
        end

      {:error, error} ->
        raise RuntimeError, to_string(error)
    end
  end

  defp create_temp_table(db, %Series{} = series) do
    series = ensure_materialized(series)
    pid_hash = :erlang.phash2(self())
    table_name = "__explorer_s_#{pid_hash}_#{System.unique_integer([:positive])}"

    # Convert series to a single-column DataFrame and register via NIF
    # This gives us __rowid for guaranteed ordering
    df_ref = Native.s_to_dataframe(series.data.resource)

    case Native.df_register_table(db, df_ref, table_name) do
      {} -> table_name
      :ok -> table_name
      {:ok, _} -> table_name
      {:error, _error} ->
        # Fallback to SQL-based insertion
        dtype_sql = ExplorerDuckDB.DataFrame.explorer_dtype_to_duckdb_sql(series.dtype)
        ExplorerDuckDB.DataFrame.execute!(db, "CREATE TEMPORARY TABLE #{table_name} (__rowid BIGINT, \"value\" #{dtype_sql})")

        values = to_list(series)

        if values != [] do
          value_converter = value_converter_for_dtype(series.dtype)

          values
          |> Enum.with_index()
          |> Enum.each(fn {v, idx} ->
            ExplorerDuckDB.DataFrame.execute!(db, "INSERT INTO #{table_name} VALUES (#{idx}, #{value_converter.(v)})")
          end)
        end

        table_name
    end
  end

  defp cleanup(db, table) do
    Native.db_execute(db, "DROP TABLE IF EXISTS #{table}")
    :ok
  end

  defp cumulative_window(series, agg_fn, reverse?) do
    db = Shared.get_db()
    table = create_temp_table(db, series)

    order = if reverse?, do: "DESC", else: "ASC"
    sql = "SELECT #{agg_fn}(\"value\") OVER (ORDER BY __rowid #{order} ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS \"value\" FROM #{table}"

    result = query_series(db, sql)
    cleanup(db, table)

    if reverse? do
      reverse(result)
    else
      result
    end
  end

  defp rolling_window(series, agg_fn, window_size, _min_periods, center) do
    db = Shared.get_db()
    table = create_temp_table(db, series)

    {preceding, following} =
      if center do
        half = div(window_size - 1, 2)
        {half, window_size - 1 - half}
      else
        {window_size - 1, 0}
      end

    sql = "SELECT #{agg_fn}(\"value\") OVER (ORDER BY __rowid ROWS BETWEEN #{preceding} PRECEDING AND #{following} FOLLOWING) AS \"value\" FROM #{table}"

    result = query_series(db, sql)
    cleanup(db, table)
    result
  end

  defp sql_window_fill(series, window_expr) do
    db = Shared.get_db()
    table = create_temp_table(db, series)
    sql = "SELECT COALESCE(\"value\", #{window_expr}) AS \"value\" FROM #{table}"
    result = query_series(db, sql)
    cleanup(db, table)
    result
  end

  defp compute_ewm_mean(list, alpha, adjust) do
    list
    |> Enum.reduce({[], 0.0, 0.0}, fn val, {acc, weighted_sum, total_weight} ->
      case val do
        nil ->
          {[nil | acc], weighted_sum, total_weight}

        v ->
          new_weighted_sum = v + (1 - alpha) * weighted_sum
          new_total_weight = 1 + (1 - alpha) * total_weight
          mean = if adjust, do: new_weighted_sum / new_total_weight, else: new_weighted_sum
          {[mean | acc], new_weighted_sum, new_total_weight}
      end
    end)
    |> elem(0)
    |> Enum.reverse()
  end

  defp binary_series_op(%Series{} = left, %Series{} = right, op) do
    left = ensure_materialized(left)
    right = ensure_materialized(right)

    left_size = size(left)
    right_size = size(right)

    cond do
      # Broadcast: right is scalar (size 1), use scalar path
      right_size == 1 and left_size > 1 ->
        scalar = at(right, 0)
        binary_series_op(left, scalar, op)

      # Broadcast: left is scalar (size 1), use scalar path
      left_size == 1 and right_size > 1 ->
        scalar = at(left, 0)
        binary_series_op(scalar, right, op)

      # Same size or both scalars: positional join
      true ->
        db = Shared.get_db()

        case Native.s_binary_op(db, left.data.resource, right.data.resource, op) do
          {:ok, ref} -> Shared.create_series(ref)
          ref when is_reference(ref) -> Shared.create_series(ref)
          {:error, error} -> raise RuntimeError, to_string(error)
        end
    end
  end

  defp binary_series_op(%Series{} = left, scalar, op) when is_number(scalar) do
    left = ensure_materialized(left)
    db = Shared.get_db()

    result =
      if is_float(scalar) do
        Native.s_binary_op_scalar_rhs_f64(db, left.data.resource, op, scalar)
      else
        Native.s_binary_op_scalar_rhs_i64(db, left.data.resource, op, scalar)
      end

    case result do
      {:ok, ref} -> Shared.create_series(ref)
      ref when is_reference(ref) -> Shared.create_series(ref)
      {:error, error} -> raise RuntimeError, to_string(error)
    end
  end

  defp binary_series_op(scalar, %Series{} = right, op) when is_number(scalar) do
    left_series = from_list(List.duplicate(scalar, size(right)), right.dtype)
    binary_series_op(left_series, right, op)
  end

  # For date types, DuckDB returns integers (days since epoch) from Arrow.
  # We need to convert them to proper date literals when inserting back.
  defp value_converter_for_dtype(:date) do
    fn
      nil -> "NULL"
      v when is_integer(v) -> "EPOCH_MS(0)::DATE + INTERVAL '#{v} days'"
      v -> value_to_sql(v)
    end
  end

  defp value_converter_for_dtype({:naive_datetime, _}) do
    fn
      nil -> "NULL"
      v when is_integer(v) -> "EPOCH_US(#{v})"
      v -> value_to_sql(v)
    end
  end

  defp value_converter_for_dtype(_), do: &value_to_sql/1

  defdelegate value_to_sql(v), to: ExplorerDuckDB.SQLHelpers
end
