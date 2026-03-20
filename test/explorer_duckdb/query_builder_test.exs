defmodule ExplorerDuckDB.QueryBuilderTest do
  use ExUnit.Case, async: true

  alias ExplorerDuckDB.QueryBuilder

  test "no operations returns source" do
    assert QueryBuilder.build("my_table", []) == "my_table"
  end

  test "single filter produces CTE" do
    sql = QueryBuilder.build("t", [{:filter, "\"age\" > 21"}])
    assert sql =~ "WITH"
    assert sql =~ "WHERE \"age\" > 21"
    assert sql =~ "SELECT * FROM __s0"
  end

  test "select columns" do
    sql = QueryBuilder.build("t", [{:select, ["a", "b"]}])
    assert sql =~ "SELECT \"a\", \"b\" FROM t"
  end

  test "limit (head)" do
    sql = QueryBuilder.build("t", [{:limit, 5}])
    assert sql =~ "LIMIT 5"
  end

  test "limit with offset (tail/slice)" do
    sql = QueryBuilder.build("t", [{:limit, 3, 10}])
    assert sql =~ "LIMIT 3 OFFSET 10"
  end

  test "drop_nil" do
    sql = QueryBuilder.build("t", [{:drop_nil, ["x", "y"]}])
    assert sql =~ "IS NOT NULL"
  end

  test "sort" do
    ops = [{:sort, [{:asc, "\"x\"", true}]}]
    sql = QueryBuilder.build("t", ops)
    assert sql =~ "ORDER BY \"x\" ASC NULLS LAST"
  end

  test "chained filter + sort + limit uses CTEs (newest-first storage)" do
    ops = [
      {:limit, 10},
      {:sort, [{:asc, "\"x\"", false}]},
      {:filter, "\"x\" > 0"}
    ]

    sql = QueryBuilder.build("t", ops)

    # Should have 3 CTE steps
    assert sql =~ "__s0"
    assert sql =~ "__s1"
    assert sql =~ "__s2"
    assert sql =~ "WHERE \"x\" > 0"
    assert sql =~ "ORDER BY"
    assert sql =~ "LIMIT 10"
  end

  test "summarise with groups" do
    ops = [{:summarise, ["dept"], [{"total", "SUM(\"salary\")"}]}]
    sql = QueryBuilder.build("t", ops)
    assert sql =~ "GROUP BY \"dept\""
    assert sql =~ "SUM(\"salary\") AS \"total\""
  end

  test "summarise without groups" do
    ops = [{:summarise, [], [{"total", "SUM(\"salary\")"}]}]
    sql = QueryBuilder.build("t", ops)
    assert sql =~ "SUM(\"salary\") AS \"total\""
    refute sql =~ "GROUP BY"
  end

  test "rename" do
    ops = [{:rename, ["a", "b"], [{"a", "x"}, {"b", "y"}]}]
    sql = QueryBuilder.build("t", ops)
    assert sql =~ "\"a\" AS \"x\""
    assert sql =~ "\"b\" AS \"y\""
  end

  test "mutate with out columns" do
    ops = [{:mutate, ["a", "b", "c"], [{"c", "(\"a\" + \"b\")"}]}]
    sql = QueryBuilder.build("t", ops)
    assert sql =~ "(\"a\" + \"b\") AS \"c\""
  end

  test "raw sql" do
    ops = [{:raw_sql, "SELECT x + 1 AS y FROM my_tbl", "my_tbl"}]
    sql = QueryBuilder.build("t", ops)
    assert sql =~ "my_tbl"
    assert sql =~ "SELECT x + 1 AS y"
  end

  test "CTE format is readable" do
    ops = [
      {:limit, 5},
      {:sort, [{:asc, "\"name\"", true}]},
      {:filter, "\"age\" > 21"},
      {:select, ["name", "age"]}
    ]

    sql = QueryBuilder.build("users", ops)

    # Should be a multi-line CTE query
    assert sql =~ "WITH __s0 AS"
    assert sql =~ "__s1 AS"
    assert sql =~ "__s2 AS"
    assert sql =~ "__s3 AS"
    assert sql =~ "SELECT * FROM __s3"
  end
end
