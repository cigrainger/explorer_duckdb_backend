defmodule ExplorerDuckDB.QueryBuilder do
  @moduledoc """
  Translates a list of accumulated lazy operations into a single DuckDB SQL query.

  Operations are stored newest-first and reversed before processing.
  Each operation wraps the previous query as a subquery.
  """

  @doc """
  Builds a SQL query string from a source and operations list.
  Returns a SQL string ready for execution.
  """
  def build(source, operations) do
    operations
    |> Enum.reverse()
    |> Enum.reduce(source, &apply_op/2)
  end

  defp apply_op({:filter, expr}, prev) do
    "(SELECT * FROM #{prev} WHERE #{expr})"
  end

  defp apply_op({:select, columns}, prev) do
    cols = cols_sql(columns)
    "(SELECT #{cols} FROM #{prev})"
  end

  defp apply_op({:mutate, out_columns, mutations}, prev) do
    mutation_map = Map.new(mutations)

    select_parts =
      Enum.map(out_columns, fn name ->
        case Map.get(mutation_map, name) do
          nil -> q(name)
          expr -> "#{expr} AS #{q(name)}"
        end
      end)

    "(SELECT #{Enum.join(select_parts, ", ")} FROM #{prev})"
  end

  defp apply_op({:sort, sort_exprs}, prev) do
    order =
      Enum.map_join(sort_exprs, ", ", fn {dir, expr, nulls_last?} ->
        dir_str = if dir == :desc, do: "DESC", else: "ASC"
        nulls_str = if nulls_last?, do: "NULLS LAST", else: "NULLS FIRST"
        "#{expr} #{dir_str} #{nulls_str}"
      end)

    "(SELECT * FROM #{prev} ORDER BY #{order})"
  end

  defp apply_op({:distinct, columns}, prev) do
    cols = cols_sql(columns)
    "(SELECT DISTINCT ON (#{cols}) * FROM #{prev})"
  end

  defp apply_op({:limit, n}, prev) do
    "(SELECT * FROM #{prev} LIMIT #{n})"
  end

  defp apply_op({:limit, n, offset}, prev) do
    "(SELECT * FROM #{prev} LIMIT #{n} OFFSET #{offset})"
  end

  defp apply_op({:rename, all_columns, renames}, prev) do
    rename_map = Map.new(renames)

    select_parts =
      Enum.map(all_columns, fn name ->
        case Map.get(rename_map, name) do
          nil -> q(name)
          new_name -> "#{q(name)} AS #{q(new_name)}"
        end
      end)

    "(SELECT #{Enum.join(select_parts, ", ")} FROM #{prev})"
  end

  defp apply_op({:drop_nil, columns}, prev) do
    where = Enum.map_join(columns, " AND ", fn col -> "#{q(col)} IS NOT NULL" end)
    "(SELECT * FROM #{prev} WHERE #{where})"
  end

  defp apply_op({:summarise, group_cols, agg_exprs}, prev) do
    group_parts = Enum.map(group_cols, &q/1)
    agg_parts = Enum.map(agg_exprs, fn {name, expr} -> "#{expr} AS #{q(name)}" end)
    select = Enum.join(group_parts ++ agg_parts, ", ")

    if group_cols == [] do
      "(SELECT #{select} FROM #{prev})"
    else
      group_by = cols_sql(group_cols)
      "(SELECT #{select} FROM #{prev} GROUP BY #{group_by})"
    end
  end

  defp apply_op({:join, right_source, how, on_pairs, select_cols}, prev) do
    join_str =
      case how do
        :inner -> "INNER JOIN"
        :left -> "LEFT JOIN"
        :right -> "RIGHT JOIN"
        :outer -> "FULL OUTER JOIN"
        :cross -> "CROSS JOIN"
      end

    on_clause =
      if how == :cross do
        ""
      else
        conds = Enum.map_join(on_pairs, " AND ", fn {l, r} -> "l.#{q(l)} = r.#{q(r)}" end)
        "ON #{conds}"
      end

    select = Enum.map_join(select_cols, ", ", fn
      {side, col, alias_name} when col != alias_name -> "#{side}.#{q(col)} AS #{q(alias_name)}"
      {side, col, _alias_name} -> "#{side}.#{q(col)}"
      {side, col} -> "#{side}.#{q(col)}"
    end)

    "(SELECT #{select} FROM #{prev} l #{join_str} #{right_source} r #{on_clause})"
  end

  defp apply_op({:union_all, other_sources, columns}, prev) do
    cols = cols_sql(columns)
    first = "SELECT #{cols} FROM #{prev}"
    rest = Enum.map(other_sources, fn src -> "SELECT #{cols} FROM #{src}" end)
    "(#{Enum.join([first | rest], " UNION ALL ")})"
  end

  defp apply_op({:nil_count, columns}, prev) do
    parts =
      Enum.map(columns, fn name ->
        "COUNT(*) FILTER (WHERE #{q(name)} IS NULL) AS #{q(name)}"
      end)

    "(SELECT #{Enum.join(parts, ", ")} FROM #{prev})"
  end

  defp apply_op({:raw_sql, sql, table_name}, prev) do
    # Replace table_name reference with the prev query
    "(WITH #{q(table_name)} AS (SELECT * FROM #{prev}) #{sql})"
  end

  # Helpers
  defp q(name), do: "\"#{name}\""
  defp cols_sql(columns), do: Enum.map_join(columns, ", ", &q/1)
end
