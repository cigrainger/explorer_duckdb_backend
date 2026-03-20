defmodule ExplorerDuckDB.SQLBuilder do
  @moduledoc false

  alias Explorer.Backend.LazySeries

  @doc """
  Convert a LazySeries operation tree into a DuckDB SQL expression string.
  """
  def to_sql(%LazySeries{op: :column, args: [name]}), do: ~s("#{name}")

  # Arithmetic
  def to_sql(%LazySeries{op: :add, args: [left, right]}),
    do: "(#{to_sql(left)} + #{to_sql(right)})"

  def to_sql(%LazySeries{op: :subtract, args: [left, right]}),
    do: "(#{to_sql(left)} - #{to_sql(right)})"

  def to_sql(%LazySeries{op: :multiply, args: [left, right]}),
    do: "(#{to_sql(left)} * #{to_sql(right)})"

  def to_sql(%LazySeries{op: :divide, args: [left, right]}),
    do: "(#{to_sql(left)} / #{to_sql(right)})"

  def to_sql(%LazySeries{op: :pow, args: [base, exp]}),
    do: "POWER(#{to_sql(base)}, #{to_sql(exp)})"

  def to_sql(%LazySeries{op: :remainder, args: [left, right]}),
    do: "(#{to_sql(left)} % #{to_sql(right)})"

  def to_sql(%LazySeries{op: :quotient, args: [left, right]}),
    do: "(#{to_sql(left)} // #{to_sql(right)})"

  def to_sql(%LazySeries{op: :abs, args: [s]}),
    do: "ABS(#{to_sql(s)})"

  def to_sql(%LazySeries{op: :negate, args: [s]}),
    do: "(-(#{to_sql(s)}))"

  # Comparisons
  def to_sql(%LazySeries{op: :equal, args: [left, right]}),
    do: "(#{to_sql(left)} = #{to_sql(right)})"

  def to_sql(%LazySeries{op: :not_equal, args: [left, right]}),
    do: "(#{to_sql(left)} != #{to_sql(right)})"

  def to_sql(%LazySeries{op: :greater, args: [left, right]}),
    do: "(#{to_sql(left)} > #{to_sql(right)})"

  def to_sql(%LazySeries{op: :greater_equal, args: [left, right]}),
    do: "(#{to_sql(left)} >= #{to_sql(right)})"

  def to_sql(%LazySeries{op: :less, args: [left, right]}),
    do: "(#{to_sql(left)} < #{to_sql(right)})"

  def to_sql(%LazySeries{op: :less_equal, args: [left, right]}),
    do: "(#{to_sql(left)} <= #{to_sql(right)})"

  # Logic
  def to_sql(%LazySeries{op: :binary_and, args: [left, right]}),
    do: "(#{to_sql(left)} AND #{to_sql(right)})"

  def to_sql(%LazySeries{op: :binary_or, args: [left, right]}),
    do: "(#{to_sql(left)} OR #{to_sql(right)})"

  def to_sql(%LazySeries{op: :unary_not, args: [s]}),
    do: "(NOT #{to_sql(s)})"

  # Null checks
  def to_sql(%LazySeries{op: :is_nil, args: [s]}),
    do: "(#{to_sql(s)} IS NULL)"

  def to_sql(%LazySeries{op: :is_not_nil, args: [s]}),
    do: "(#{to_sql(s)} IS NOT NULL)"

  # Aggregations
  def to_sql(%LazySeries{op: :sum, args: [s]}),
    do: "SUM(#{to_sql(s)})"

  def to_sql(%LazySeries{op: :mean, args: [s]}),
    do: "AVG(#{to_sql(s)})"

  def to_sql(%LazySeries{op: :min, args: [s]}),
    do: "MIN(#{to_sql(s)})"

  def to_sql(%LazySeries{op: :max, args: [s]}),
    do: "MAX(#{to_sql(s)})"

  def to_sql(%LazySeries{op: :count, args: [s]}),
    do: "COUNT(#{to_sql(s)})"

  def to_sql(%LazySeries{op: :nil_count, args: [s]}),
    do: "COUNT(*) FILTER (WHERE #{to_sql(s)} IS NULL)"

  def to_sql(%LazySeries{op: :n_distinct, args: [s]}),
    do: "COUNT(DISTINCT #{to_sql(s)})"

  def to_sql(%LazySeries{op: :median, args: [s]}),
    do: "MEDIAN(#{to_sql(s)})"

  def to_sql(%LazySeries{op: :variance, args: [s, _ddof]}),
    do: "VARIANCE(#{to_sql(s)})"

  def to_sql(%LazySeries{op: :standard_deviation, args: [s, _ddof]}),
    do: "STDDEV(#{to_sql(s)})"

  def to_sql(%LazySeries{op: :first, args: [s]}),
    do: "FIRST(#{to_sql(s)})"

  def to_sql(%LazySeries{op: :last, args: [s]}),
    do: "LAST(#{to_sql(s)})"

  # Cast
  def to_sql(%LazySeries{op: :cast, args: [s, dtype]}),
    do: "CAST(#{to_sql(s)} AS #{dtype_to_duckdb_sql(dtype)})"

  # String operations
  def to_sql(%LazySeries{op: :contains, args: [s, pattern]}),
    do: "contains(#{to_sql(s)}, #{escape_string(pattern)})"

  def to_sql(%LazySeries{op: :upcase, args: [s]}),
    do: "UPPER(#{to_sql(s)})"

  def to_sql(%LazySeries{op: :downcase, args: [s]}),
    do: "LOWER(#{to_sql(s)})"

  def to_sql(%LazySeries{op: :strip, args: [s, _chars]}),
    do: "TRIM(#{to_sql(s)})"

  # Date/time extraction
  def to_sql(%LazySeries{op: :year, args: [s]}),
    do: "EXTRACT(YEAR FROM #{to_sql(s)})"

  def to_sql(%LazySeries{op: :month, args: [s]}),
    do: "EXTRACT(MONTH FROM #{to_sql(s)})"

  def to_sql(%LazySeries{op: :day_of_month, args: [s]}),
    do: "EXTRACT(DAY FROM #{to_sql(s)})"

  def to_sql(%LazySeries{op: :hour, args: [s]}),
    do: "EXTRACT(HOUR FROM #{to_sql(s)})"

  def to_sql(%LazySeries{op: :minute, args: [s]}),
    do: "EXTRACT(MINUTE FROM #{to_sql(s)})"

  def to_sql(%LazySeries{op: :second, args: [s]}),
    do: "EXTRACT(SECOND FROM #{to_sql(s)})"

  def to_sql(%LazySeries{op: :day_of_week, args: [s]}),
    do: "EXTRACT(DOW FROM #{to_sql(s)})"

  # Rounding
  def to_sql(%LazySeries{op: :round, args: [s, decimals]}),
    do: "ROUND(#{to_sql(s)}, #{decimals})"

  def to_sql(%LazySeries{op: :floor, args: [s]}),
    do: "FLOOR(#{to_sql(s)})"

  def to_sql(%LazySeries{op: :ceil, args: [s]}),
    do: "CEIL(#{to_sql(s)})"

  # Scalar values
  def to_sql(nil), do: "NULL"
  def to_sql(true), do: "TRUE"
  def to_sql(false), do: "FALSE"
  def to_sql(value) when is_integer(value), do: Integer.to_string(value)
  def to_sql(value) when is_float(value), do: Float.to_string(value)
  def to_sql(value) when is_binary(value), do: escape_string(value)
  def to_sql(:nan), do: "'NaN'::DOUBLE"
  def to_sql(:infinity), do: "'Infinity'::DOUBLE"
  def to_sql(:neg_infinity), do: "'-Infinity'::DOUBLE"

  # from_list wraps a scalar value in a lazy series
  def to_sql(%LazySeries{op: :from_list, args: [list, _dtype]}) when is_list(list) do
    case list do
      [single] -> to_sql(single)
      _ -> raise ArgumentError, "from_list with multiple values not supported in SQL context"
    end
  end

  # Series passed directly (e.g., scalar broadcast)
  def to_sql(%Explorer.Series{} = series) do
    # Extract scalar value for single-element series
    if Explorer.Series.size(series) == 1 do
      to_sql(Explorer.Series.first(series))
    else
      raise ArgumentError, "cannot convert multi-element Series to SQL expression"
    end
  end

  # Fallback for unsupported operations
  def to_sql(%LazySeries{op: op}) do
    raise ArgumentError, "DuckDB SQL builder does not yet support operation: #{inspect(op)}"
  end

  # Helpers

  defp escape_string(s) when is_binary(s) do
    escaped = String.replace(s, "'", "''")
    "'#{escaped}'"
  end

  defp dtype_to_duckdb_sql({:s, 8}), do: "TINYINT"
  defp dtype_to_duckdb_sql({:s, 16}), do: "SMALLINT"
  defp dtype_to_duckdb_sql({:s, 32}), do: "INTEGER"
  defp dtype_to_duckdb_sql({:s, 64}), do: "BIGINT"
  defp dtype_to_duckdb_sql({:u, 8}), do: "UTINYINT"
  defp dtype_to_duckdb_sql({:u, 16}), do: "USMALLINT"
  defp dtype_to_duckdb_sql({:u, 32}), do: "UINTEGER"
  defp dtype_to_duckdb_sql({:u, 64}), do: "UBIGINT"
  defp dtype_to_duckdb_sql({:f, 32}), do: "FLOAT"
  defp dtype_to_duckdb_sql({:f, 64}), do: "DOUBLE"
  defp dtype_to_duckdb_sql(:boolean), do: "BOOLEAN"
  defp dtype_to_duckdb_sql(:string), do: "VARCHAR"
  defp dtype_to_duckdb_sql(:date), do: "DATE"
  defp dtype_to_duckdb_sql({:naive_datetime, _}), do: "TIMESTAMP"
  defp dtype_to_duckdb_sql({:datetime, _, _}), do: "TIMESTAMPTZ"
  defp dtype_to_duckdb_sql(_), do: "VARCHAR"
end
