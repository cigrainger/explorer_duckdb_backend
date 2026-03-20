defmodule ExplorerDuckDB.SQLHelpers do
  @moduledoc false

  @doc """
  Escape a SQL identifier (column name, table name) by doubling internal quotes.
  """
  def quote_ident(name) do
    escaped = name |> to_string() |> String.replace("\"", "\"\"")
    "\"#{escaped}\""
  end

  @doc """
  Escape a SQL string literal by doubling single quotes.
  """
  def quote_string(value) do
    escaped = value |> to_string() |> String.replace("'", "''")
    "'#{escaped}'"
  end

  @doc """
  Convert a value to a SQL literal, with proper escaping.
  """
  def value_to_sql(nil), do: "NULL"
  def value_to_sql(true), do: "TRUE"
  def value_to_sql(false), do: "FALSE"
  def value_to_sql(v) when is_integer(v), do: Integer.to_string(v)
  def value_to_sql(v) when is_float(v), do: Float.to_string(v)
  def value_to_sql(:nan), do: "'NaN'::DOUBLE"
  def value_to_sql(:infinity), do: "'Infinity'::DOUBLE"
  def value_to_sql(:neg_infinity), do: "'-Infinity'::DOUBLE"
  def value_to_sql(v) when is_binary(v), do: quote_string(v)
  def value_to_sql(%Date{} = d), do: "'#{Date.to_iso8601(d)}'::DATE"
  def value_to_sql(%NaiveDateTime{} = dt), do: "'#{NaiveDateTime.to_iso8601(dt)}'::TIMESTAMP"
  def value_to_sql(%DateTime{} = dt), do: "'#{DateTime.to_iso8601(dt)}'::TIMESTAMPTZ"
  def value_to_sql(v), do: quote_string(Kernel.inspect(v))
end
