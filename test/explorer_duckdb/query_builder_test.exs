defmodule ExplorerDuckDB.QueryBuilderTest do
  use ExUnit.Case, async: true

  alias ExplorerDuckDB.QueryBuilder

  test "no operations returns source" do
    assert QueryBuilder.build("my_table", []) == "my_table"
  end

  test "single filter" do
    sql = QueryBuilder.build("t", [{:filter, "\"age\" > 21"}])
    assert sql == "(SELECT * FROM t WHERE \"age\" > 21)"
  end

  test "select columns" do
    sql = QueryBuilder.build("t", [{:select, ["a", "b"]}])
    assert sql == "(SELECT \"a\", \"b\" FROM t)"
  end

  test "limit (head)" do
    sql = QueryBuilder.build("t", [{:limit, 5}])
    assert sql == "(SELECT * FROM t LIMIT 5)"
  end

  test "limit with offset (tail/slice)" do
    sql = QueryBuilder.build("t", [{:limit, 3, 10}])
    assert sql == "(SELECT * FROM t LIMIT 3 OFFSET 10)"
  end

  test "drop_nil" do
    sql = QueryBuilder.build("t", [{:drop_nil, ["x", "y"]}])
    assert sql == "(SELECT * FROM t WHERE \"x\" IS NOT NULL AND \"y\" IS NOT NULL)"
  end

  test "sort" do
    ops = [{:sort, [{:asc, "\"x\"", true}]}]
    sql = QueryBuilder.build("t", ops)
    assert sql == "(SELECT * FROM t ORDER BY \"x\" ASC NULLS LAST)"
  end

  test "chained filter + sort + limit (newest-first storage)" do
    # Operations are stored newest-first: limit was added last, filter first
    ops = [
      {:limit, 10},
      {:sort, [{:asc, "\"x\"", false}]},
      {:filter, "\"x\" > 0"}
    ]

    sql = QueryBuilder.build("t", ops)

    # Build reverses to oldest-first: filter(t) -> sort -> limit
    assert sql ==
             "(SELECT * FROM (SELECT * FROM (SELECT * FROM t WHERE \"x\" > 0) ORDER BY \"x\" ASC NULLS FIRST) LIMIT 10)"
  end

  test "summarise with groups" do
    ops = [{:summarise, ["dept"], [{"total", "SUM(\"salary\")"}]}]
    sql = QueryBuilder.build("t", ops)
    assert sql == "(SELECT \"dept\", SUM(\"salary\") AS \"total\" FROM t GROUP BY \"dept\")"
  end

  test "summarise without groups" do
    ops = [{:summarise, [], [{"total", "SUM(\"salary\")"}]}]
    sql = QueryBuilder.build("t", ops)
    assert sql == "(SELECT SUM(\"salary\") AS \"total\" FROM t)"
  end

  test "rename" do
    ops = [{:rename, ["a", "b"], [{"a", "x"}, {"b", "y"}]}]
    sql = QueryBuilder.build("t", ops)
    assert sql == "(SELECT \"a\" AS \"x\", \"b\" AS \"y\" FROM t)"
  end

  test "mutate with out columns" do
    ops = [{:mutate, ["a", "b", "c"], [{"c", "(\"a\" + \"b\")"}]}]
    sql = QueryBuilder.build("t", ops)
    assert sql == "(SELECT \"a\", \"b\", (\"a\" + \"b\") AS \"c\" FROM t)"
  end

  test "raw sql" do
    ops = [{:raw_sql, "SELECT x + 1 AS y FROM my_tbl", "my_tbl"}]
    sql = QueryBuilder.build("t", ops)
    assert sql == "(WITH \"my_tbl\" AS (SELECT * FROM t) SELECT x + 1 AS y FROM my_tbl)"
  end
end
