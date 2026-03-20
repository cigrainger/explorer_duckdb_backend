defmodule ExplorerDuckDB.Shared do
  @moduledoc false

  alias ExplorerDuckDB.Native

  import Kernel, except: [apply: 2]

  def apply(fun, args \\ []) do
    case Kernel.apply(Native, fun, args) do
      {:ok, value} -> value
      {:error, error} -> raise runtime_error(error)
    end
  end

  def create_series(duckdb_series) when is_reference(duckdb_series) do
    dtype =
      case Native.s_dtype(duckdb_series) do
        {:ok, dtype} -> dtype
        {:error, reason} -> raise ArgumentError, reason
        dtype -> dtype
      end

    backend_series = %ExplorerDuckDB.Series{resource: duckdb_series}
    Explorer.Backend.Series.new(backend_series, dtype)
  end

  def create_series(%ExplorerDuckDB.Series{} = backend_series) do
    dtype =
      case Native.s_dtype(backend_series.resource) do
        {:ok, dtype} -> dtype
        {:error, reason} -> raise ArgumentError, reason
        dtype -> dtype
      end

    Explorer.Backend.Series.new(backend_series, dtype)
  end

  def create_dataframe(duckdb_df) when is_reference(duckdb_df) do
    backend_df = %ExplorerDuckDB.DataFrame{resource: duckdb_df}
    create_dataframe(backend_df)
  end

  def create_dataframe(%ExplorerDuckDB.DataFrame{} = backend_df) do
    ref = backend_df.resource

    names =
      case Native.df_names(ref) do
        {:ok, names} -> names
        names when is_list(names) -> names
        {:error, error} -> raise runtime_error(error)
      end

    dtypes =
      case Native.df_dtypes(ref) do
        {:ok, dtypes} -> dtypes
        dtypes when is_list(dtypes) -> dtypes
        {:error, error} -> raise runtime_error(error)
      end

    {:ok, Explorer.Backend.DataFrame.new(backend_df, names, dtypes)}
  end

  def create_dataframe!(duckdb_df) do
    case create_dataframe(duckdb_df) do
      {:ok, df} -> df
      {:error, error} -> raise error
    end
  end

  @doc """
  Get or create a shared DuckDB database connection for this process.
  """
  def get_db do
    case Process.get(:explorer_duckdb_db) do
      nil ->
        # Check application config for a default database path
        path = Application.get_env(:explorer_duckdb_backend, :database, :memory)

        result =
          case path do
            :memory -> Native.db_open()
            path when is_binary(path) -> Native.db_open_path(path)
          end

        case result do
          {:ok, db} ->
            Process.put(:explorer_duckdb_db, db)
            db

          db when is_reference(db) ->
            Process.put(:explorer_duckdb_db, db)
            db

          {:error, error} ->
            raise runtime_error(error)
        end

      db ->
        db
    end
  end

  def from_list(list, dtype), do: from_list(list, dtype, "")

  def from_list(list, dtype, name) when is_list(list) do
    case dtype do
      {:s, 8} -> Native.s_from_list_s8(name, list)
      {:s, 16} -> Native.s_from_list_s16(name, list)
      {:s, 32} -> Native.s_from_list_s32(name, list)
      {:s, 64} -> Native.s_from_list_s64(name, list)
      {:u, 8} -> Native.s_from_list_u8(name, list)
      {:u, 16} -> Native.s_from_list_u16(name, list)
      {:u, 32} -> Native.s_from_list_u32(name, list)
      {:u, 64} -> Native.s_from_list_u64(name, list)
      {:f, 32} -> Native.s_from_list_f32(name, list)
      {:f, 64} -> Native.s_from_list_f64(name, list)
      :boolean -> Native.s_from_list_bool(name, list)
      :string -> Native.s_from_list_str(name, list)
      :category -> Native.s_from_list_str(name, list)
      :date ->
        str_list = Enum.map(list, fn nil -> nil; %Date{} = d -> Date.to_iso8601(d); v -> to_string(v) end)
        Native.s_from_list_str(name, str_list)
      :time ->
        str_list = Enum.map(list, fn nil -> nil; %Time{} = t -> Time.to_iso8601(t); v -> to_string(v) end)
        Native.s_from_list_str(name, str_list)
      {:naive_datetime, _} ->
        str_list = Enum.map(list, fn nil -> nil; %NaiveDateTime{} = dt -> NaiveDateTime.to_iso8601(dt); v -> to_string(v) end)
        Native.s_from_list_str(name, str_list)
      {:datetime, _, _} ->
        str_list = Enum.map(list, fn nil -> nil; %DateTime{} = dt -> DateTime.to_iso8601(dt); v -> to_string(v) end)
        Native.s_from_list_str(name, str_list)
      _ -> {:error, "unsupported dtype: #{inspect(dtype)}"}
    end
  end

  defp runtime_error(error) when is_binary(error), do: RuntimeError.exception(error)
  defp runtime_error(error), do: RuntimeError.exception(inspect(error))
end
