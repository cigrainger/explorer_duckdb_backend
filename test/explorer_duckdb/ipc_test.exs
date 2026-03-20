defmodule ExplorerDuckDB.IPCTest do
  use ExUnit.Case, async: true

  require Explorer.DataFrame
  alias Explorer.DataFrame
  alias Explorer.Series

  setup do
    Explorer.Backend.put(ExplorerDuckDB)
    :ok
  end

  describe "IPC dump/load (in-memory)" do
    test "round-trips integers" do
      df = DataFrame.new(x: [1, 2, 3], y: [4, 5, 6])
      {:ok, binary} = DataFrame.dump_ipc(df)
      assert is_binary(binary)
      assert byte_size(binary) > 0

      {:ok, loaded} = DataFrame.load_ipc(binary)
      assert DataFrame.n_rows(loaded) == 3
      assert Series.to_list(DataFrame.pull(loaded, "x")) == [1, 2, 3]
      assert Series.to_list(DataFrame.pull(loaded, "y")) == [4, 5, 6]
    end

    test "round-trips floats" do
      df = DataFrame.new(x: [1.1, 2.2, 3.3])
      {:ok, binary} = DataFrame.dump_ipc(df)
      {:ok, loaded} = DataFrame.load_ipc(binary)

      values = Series.to_list(DataFrame.pull(loaded, "x"))
      assert_in_delta hd(values), 1.1, 0.001
    end

    test "round-trips strings" do
      df = DataFrame.new(name: ["Alice", "Bob", "Carol"])
      {:ok, binary} = DataFrame.dump_ipc(df)
      {:ok, loaded} = DataFrame.load_ipc(binary)

      assert Series.to_list(DataFrame.pull(loaded, "name")) == ["Alice", "Bob", "Carol"]
    end

    test "round-trips with nils" do
      df = DataFrame.new(x: [1, nil, 3, nil, 5])
      {:ok, binary} = DataFrame.dump_ipc(df)
      {:ok, loaded} = DataFrame.load_ipc(binary)

      assert Series.to_list(DataFrame.pull(loaded, "x")) == [1, nil, 3, nil, 5]
    end

    test "round-trips mixed types" do
      df = DataFrame.new(
        ints: [1, 2, 3],
        floats: [1.1, 2.2, 3.3],
        strings: ["a", "b", "c"],
        bools: [true, false, true]
      )

      {:ok, binary} = DataFrame.dump_ipc(df)
      {:ok, loaded} = DataFrame.load_ipc(binary)

      assert DataFrame.n_rows(loaded) == 3
      # Column order from DataFrame.new may vary, but IPC preserves whatever it gets
      assert length(DataFrame.names(loaded)) == 4
      assert "ints" in DataFrame.names(loaded)
      assert "bools" in DataFrame.names(loaded)
    end

    test "preserves column order through IPC roundtrip" do
      # Use a DataFrame where we know the column order
      df = DataFrame.new(x: [1, 2], y: [3, 4])
      {:ok, binary} = DataFrame.dump_ipc(df)
      {:ok, loaded} = DataFrame.load_ipc(binary)

      assert DataFrame.names(loaded) == DataFrame.names(df)
    end
  end

  describe "IPC Stream dump/load" do
    test "round-trips data" do
      df = DataFrame.new(x: [10, 20, 30], y: ["a", "b", "c"])
      {:ok, binary} = DataFrame.dump_ipc_stream(df)
      assert is_binary(binary)

      {:ok, loaded} = DataFrame.load_ipc_stream(binary)
      assert DataFrame.n_rows(loaded) == 3
      assert Series.to_list(DataFrame.pull(loaded, "x")) == [10, 20, 30]
    end
  end

  describe "IPC file IO" do
    @tag :tmp_dir
    test "write and read IPC file", %{tmp_dir: tmp_dir} do
      path = Path.join(tmp_dir, "data.arrow")
      df = DataFrame.new(x: [1, 2, 3], y: ["a", "b", "c"])
      DataFrame.to_ipc(df, path)

      assert File.exists?(path)

      loaded = DataFrame.from_ipc!(path)
      assert DataFrame.n_rows(loaded) == 3
      assert Series.to_list(DataFrame.pull(loaded, "x")) == [1, 2, 3]
    end

    @tag :tmp_dir
    test "write and read IPC stream file", %{tmp_dir: tmp_dir} do
      path = Path.join(tmp_dir, "data.arrows")
      df = DataFrame.new(x: [1, 2, 3])
      DataFrame.to_ipc_stream(df, path)

      assert File.exists?(path)

      loaded = DataFrame.from_ipc_stream!(path)
      assert DataFrame.n_rows(loaded) == 3
    end
  end

  describe "cross-format IPC" do
    test "CSV -> IPC -> CSV roundtrip" do
      df = DataFrame.new(x: [1, 2, 3], name: ["a", "b", "c"])
      {:ok, ipc} = DataFrame.dump_ipc(df)
      {:ok, from_ipc} = DataFrame.load_ipc(ipc)
      {:ok, csv} = DataFrame.dump_csv(from_ipc)
      {:ok, from_csv} = DataFrame.load_csv(csv)

      assert DataFrame.n_rows(from_csv) == 3
    end

    test "IPC preserves data through lazy pipeline" do
      df = DataFrame.new(x: [5, 3, 1, 4, 2])

      result =
        df
        |> DataFrame.sort_by(asc: x)
        |> DataFrame.head(3)

      {:ok, ipc} = DataFrame.dump_ipc(result)
      {:ok, loaded} = DataFrame.load_ipc(ipc)

      assert Series.to_list(DataFrame.pull(loaded, "x")) == [1, 2, 3]
    end
  end
end
