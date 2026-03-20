defmodule ExplorerDuckDB.SQLBuilderTest do
  use ExUnit.Case, async: true

  alias ExplorerDuckDB.SQLBuilder
  alias Explorer.Backend.LazySeries

  test "column reference" do
    lazy = %LazySeries{op: :column, args: ["name"], dtype: :string}
    assert SQLBuilder.to_sql(lazy) == "\"name\""
  end

  test "arithmetic operations" do
    col_a = %LazySeries{op: :column, args: ["a"], dtype: {:s, 64}}
    col_b = %LazySeries{op: :column, args: ["b"], dtype: {:s, 64}}

    add = %LazySeries{op: :add, args: [col_a, col_b], dtype: {:s, 64}}
    assert SQLBuilder.to_sql(add) == "(\"a\" + \"b\")"

    sub = %LazySeries{op: :subtract, args: [col_a, col_b], dtype: {:s, 64}}
    assert SQLBuilder.to_sql(sub) == "(\"a\" - \"b\")"

    mul = %LazySeries{op: :multiply, args: [col_a, col_b], dtype: {:s, 64}}
    assert SQLBuilder.to_sql(mul) == "(\"a\" * \"b\")"

    div_op = %LazySeries{op: :divide, args: [col_a, col_b], dtype: {:f, 64}}
    assert SQLBuilder.to_sql(div_op) == "(\"a\" / \"b\")"
  end

  test "comparison operations" do
    col = %LazySeries{op: :column, args: ["age"], dtype: {:s, 64}}

    gt = %LazySeries{op: :greater, args: [col, 21], dtype: :boolean}
    assert SQLBuilder.to_sql(gt) == "(\"age\" > 21)"

    eq = %LazySeries{op: :equal, args: [col, 42], dtype: :boolean}
    assert SQLBuilder.to_sql(eq) == "(\"age\" = 42)"
  end

  test "logical operations" do
    col_a = %LazySeries{op: :column, args: ["a"], dtype: :boolean}
    col_b = %LazySeries{op: :column, args: ["b"], dtype: :boolean}

    and_op = %LazySeries{op: :binary_and, args: [col_a, col_b], dtype: :boolean}
    assert SQLBuilder.to_sql(and_op) == "(\"a\" AND \"b\")"

    not_op = %LazySeries{op: :unary_not, args: [col_a], dtype: :boolean}
    assert SQLBuilder.to_sql(not_op) == "(NOT \"a\")"
  end

  test "null checks" do
    col = %LazySeries{op: :column, args: ["x"], dtype: {:s, 64}}

    is_nil_op = %LazySeries{op: :is_nil, args: [col], dtype: :boolean}
    assert SQLBuilder.to_sql(is_nil_op) == "(\"x\" IS NULL)"

    is_not_nil = %LazySeries{op: :is_not_nil, args: [col], dtype: :boolean}
    assert SQLBuilder.to_sql(is_not_nil) == "(\"x\" IS NOT NULL)"
  end

  test "aggregation functions" do
    col = %LazySeries{op: :column, args: ["val"], dtype: {:s, 64}}

    sum = %LazySeries{op: :sum, args: [col], dtype: {:s, 64}, aggregation: true}
    assert SQLBuilder.to_sql(sum) == "SUM(\"val\")"

    avg = %LazySeries{op: :mean, args: [col], dtype: {:f, 64}, aggregation: true}
    assert SQLBuilder.to_sql(avg) == "AVG(\"val\")"

    count = %LazySeries{op: :count, args: [col], dtype: {:u, 32}, aggregation: true}
    assert SQLBuilder.to_sql(count) == "COUNT(\"val\")"
  end

  test "scalar values" do
    assert SQLBuilder.to_sql(42) == "42"
    assert SQLBuilder.to_sql(3.14) == "3.14"
    assert SQLBuilder.to_sql(nil) == "NULL"
    assert SQLBuilder.to_sql(true) == "TRUE"
    assert SQLBuilder.to_sql(false) == "FALSE"
    assert SQLBuilder.to_sql("hello") == "'hello'"
  end

  test "nested expressions" do
    col_a = %LazySeries{op: :column, args: ["a"], dtype: {:s, 64}}
    col_b = %LazySeries{op: :column, args: ["b"], dtype: {:s, 64}}
    sum = %LazySeries{op: :add, args: [col_a, col_b], dtype: {:s, 64}}
    gt = %LazySeries{op: :greater, args: [sum, 10], dtype: :boolean}

    assert SQLBuilder.to_sql(gt) == "((\"a\" + \"b\") > 10)"
  end
end
