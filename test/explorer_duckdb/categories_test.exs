defmodule ExplorerDuckDB.CategoriesTest do
  use ExUnit.Case, async: true

  require Explorer.DataFrame
  alias Explorer.DataFrame
  alias Explorer.Series

  setup do
    Explorer.Backend.put(ExplorerDuckDB)
    :ok
  end

  describe "categories" do
    test "returns unique values from category series" do
      s = Series.from_list(["a", "b", "a", "c", "b", "a"], dtype: :category)
      cats = Series.categories(s)
      cat_list = Series.to_list(cats) |> Enum.sort()
      assert cat_list == ["a", "b", "c"]
    end

    test "excludes nil from categories" do
      s = Series.from_list(["x", nil, "y", nil, "x"], dtype: :category)
      cats = Series.categories(s)
      cat_list = Series.to_list(cats) |> Enum.sort()
      assert cat_list == ["x", "y"]
    end
  end

  describe "categorise" do
    test "maps integer indices to category values" do
      categories = Series.from_list(["red", "green", "blue"])
      indices = Series.from_list([0, 2, 1, 0, 2])
      result = Series.categorise(indices, categories)

      assert Series.to_list(result) == ["red", "blue", "green", "red", "blue"]
      assert Series.dtype(result) == :category
    end

    test "handles nil indices" do
      categories = Series.from_list(["a", "b"])
      indices = Series.from_list([0, nil, 1])
      result = Series.categorise(indices, categories)

      assert Series.to_list(result) == ["a", nil, "b"]
    end

    test "out of range indices become nil" do
      categories = Series.from_list(["a", "b"])
      indices = Series.from_list([0, 5, 1])
      result = Series.categorise(indices, categories)

      assert Series.to_list(result) == ["a", nil, "b"]
    end
  end

  describe "from_list with category dtype" do
    test "creates category series from strings" do
      s = Series.from_list(["x", "y", "x", "z"], dtype: :category)
      assert Series.size(s) == 4
      # Stored as strings internally
      assert Series.to_list(s) == ["x", "y", "x", "z"]
    end
  end

  describe "cut" do
    test "bins values into categories" do
      s = Series.from_list([1.0, 5.0, 10.0, 15.0, 20.0])
      result = Series.cut(s, [5.0, 15.0])

      assert DataFrame.n_rows(result) == 5
      assert "break_point" in DataFrame.names(result)
      assert "category" in DataFrame.names(result)
    end

    test "cut with custom labels" do
      s = Series.from_list([1.0, 5.0, 10.0, 15.0, 20.0])
      result = Series.cut(s, [10.0], labels: ["low", "high"])

      cats = Series.to_list(DataFrame.pull(result, "category"))
      assert "low" in cats
      assert "high" in cats
    end

    test "cut with custom column names" do
      s = Series.from_list([1.0, 5.0, 10.0])
      result = Series.cut(s, [5.0], break_point_label: "val", category_label: "bin")

      assert "val" in DataFrame.names(result)
      assert "bin" in DataFrame.names(result)
    end
  end

  describe "qcut" do
    test "bins values by quantiles" do
      s = Series.from_list([1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0])
      result = Series.qcut(s, [0.25, 0.75])

      assert DataFrame.n_rows(result) == 10
      assert "category" in DataFrame.names(result)
    end

    test "qcut with labels" do
      s = Series.from_list(Enum.map(1..100, &(&1 * 1.0)))
      result = Series.qcut(s, [0.5], labels: ["bottom_half", "top_half"])

      cats = Series.to_list(DataFrame.pull(result, "category"))
      assert "bottom_half" in cats
      assert "top_half" in cats
    end
  end
end
