defmodule ExplorerDuckDB.SeriesTest do
  use ExUnit.Case, async: true

  setup do
    Explorer.Backend.put(ExplorerDuckDB)
    :ok
  end

  describe "from_list/to_list" do
    test "integers" do
      s = Explorer.Series.from_list([1, 2, 3, 4, 5])
      assert Explorer.Series.to_list(s) == [1, 2, 3, 4, 5]
      assert Explorer.Series.dtype(s) == {:s, 64}
    end

    test "integers with nils" do
      s = Explorer.Series.from_list([1, nil, 3, nil, 5])
      assert Explorer.Series.to_list(s) == [1, nil, 3, nil, 5]
    end

    test "floats" do
      s = Explorer.Series.from_list([1.0, 2.5, 3.14])
      assert Explorer.Series.to_list(s) == [1.0, 2.5, 3.14]
      assert Explorer.Series.dtype(s) == {:f, 64}
    end

    test "booleans" do
      s = Explorer.Series.from_list([true, false, true])
      assert Explorer.Series.to_list(s) == [true, false, true]
      assert Explorer.Series.dtype(s) == :boolean
    end

    test "strings" do
      s = Explorer.Series.from_list(["hello", "world", "foo"])
      assert Explorer.Series.to_list(s) == ["hello", "world", "foo"]
      assert Explorer.Series.dtype(s) == :string
    end
  end

  describe "introspection" do
    test "size" do
      s = Explorer.Series.from_list([1, 2, 3])
      assert Explorer.Series.size(s) == 3
    end

    test "size of empty series" do
      s = Explorer.Series.from_list([], dtype: {:s, 64})
      assert Explorer.Series.size(s) == 0
    end
  end

  describe "aggregations" do
    test "sum" do
      s = Explorer.Series.from_list([1, 2, 3, 4, 5])
      assert Explorer.Series.sum(s) == 15
    end

    test "mean" do
      s = Explorer.Series.from_list([1.0, 2.0, 3.0])
      assert Explorer.Series.mean(s) == 2.0
    end

    test "min and max" do
      s = Explorer.Series.from_list([3, 1, 4, 1, 5])
      assert Explorer.Series.min(s) == 1
      assert Explorer.Series.max(s) == 5
    end

    test "median" do
      s = Explorer.Series.from_list([1.0, 2.0, 3.0, 4.0, 5.0])
      assert Explorer.Series.median(s) == 3.0
    end

    test "count" do
      s = Explorer.Series.from_list([1, 2, 3])
      assert Explorer.Series.count(s) == 3
    end
  end

  describe "slice and dice" do
    test "head" do
      s = Explorer.Series.from_list([1, 2, 3, 4, 5])
      result = Explorer.Series.head(s, 3)
      assert Explorer.Series.to_list(result) == [1, 2, 3]
    end

    test "tail" do
      s = Explorer.Series.from_list([1, 2, 3, 4, 5])
      result = Explorer.Series.tail(s, 2)
      assert Explorer.Series.to_list(result) == [4, 5]
    end

    test "slice with offset and length" do
      s = Explorer.Series.from_list([1, 2, 3, 4, 5])
      result = Explorer.Series.slice(s, 1, 3)
      assert Explorer.Series.to_list(result) == [2, 3, 4]
    end

    test "first and last" do
      s = Explorer.Series.from_list([10, 20, 30])
      assert Explorer.Series.first(s) == 10
      assert Explorer.Series.last(s) == 30
    end

    test "at" do
      s = Explorer.Series.from_list([10, 20, 30])
      assert Explorer.Series.at(s, 0) == 10
      assert Explorer.Series.at(s, 2) == 30
    end
  end

  describe "sort" do
    test "ascending" do
      s = Explorer.Series.from_list([3, 1, 4, 1, 5])
      result = Explorer.Series.sort(s)
      assert Explorer.Series.to_list(result) == [1, 1, 3, 4, 5]
    end

    test "descending" do
      s = Explorer.Series.from_list([3, 1, 4, 1, 5])
      result = Explorer.Series.sort(s, direction: :desc)
      assert Explorer.Series.to_list(result) == [5, 4, 3, 1, 1]
    end

    test "reverse" do
      s = Explorer.Series.from_list([1, 2, 3])
      result = Explorer.Series.reverse(s)
      assert Explorer.Series.to_list(result) == [3, 2, 1]
    end
  end

  describe "comparisons" do
    test "equal" do
      s1 = Explorer.Series.from_list([1, 2, 3])
      s2 = Explorer.Series.from_list([1, 0, 3])
      result = Explorer.Series.equal(s1, s2)
      assert Explorer.Series.to_list(result) == [true, false, true]
    end

    test "greater" do
      s1 = Explorer.Series.from_list([1, 2, 3])
      s2 = Explorer.Series.from_list([0, 2, 4])
      result = Explorer.Series.greater(s1, s2)
      assert Explorer.Series.to_list(result) == [true, false, false]
    end
  end

  describe "arithmetic" do
    test "add two series" do
      s1 = Explorer.Series.from_list([1, 2, 3])
      s2 = Explorer.Series.from_list([10, 20, 30])
      result = Explorer.Series.add(s1, s2)
      assert Explorer.Series.to_list(result) == [11, 22, 33]
    end

    test "subtract two series" do
      s1 = Explorer.Series.from_list([10, 20, 30])
      s2 = Explorer.Series.from_list([1, 2, 3])
      result = Explorer.Series.subtract(s1, s2)
      assert Explorer.Series.to_list(result) == [9, 18, 27]
    end

    test "multiply two series" do
      s1 = Explorer.Series.from_list([2, 3, 4])
      s2 = Explorer.Series.from_list([5, 6, 7])
      result = Explorer.Series.multiply(s1, s2)
      assert Explorer.Series.to_list(result) == [10, 18, 28]
    end
  end

  describe "null operations" do
    test "is_nil" do
      s = Explorer.Series.from_list([1, nil, 3])
      result = Explorer.Series.is_nil(s)
      assert Explorer.Series.to_list(result) == [false, true, false]
    end

    test "is_not_nil" do
      s = Explorer.Series.from_list([1, nil, 3])
      result = Explorer.Series.is_not_nil(s)
      assert Explorer.Series.to_list(result) == [true, false, true]
    end
  end

  describe "boolean operations" do
    test "all?" do
      assert Explorer.Series.all?(Explorer.Series.from_list([true, true, true]))
      refute Explorer.Series.all?(Explorer.Series.from_list([true, false, true]))
    end

    test "any?" do
      assert Explorer.Series.any?(Explorer.Series.from_list([false, true, false]))
      refute Explorer.Series.any?(Explorer.Series.from_list([false, false, false]))
    end
  end
end
