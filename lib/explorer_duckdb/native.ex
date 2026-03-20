defmodule ExplorerDuckDB.Native do
  @moduledoc false

  mode = if Mix.env() in [:dev, :test], do: :debug, else: :release

  target_dir =
    try do
      Application.app_dir(:explorer_duckdb_backend, "native/explorer_duckdb")
    rescue
      ArgumentError ->
        Path.join([Mix.Project.build_path(), "lib", "explorer_duckdb_backend", "native", "explorer_duckdb"])
    end

  use Rustler,
    otp_app: :explorer_duckdb_backend,
    crate: "explorer_duckdb",
    path: "native/explorer_duckdb",
    mode: mode,
    target_dir: target_dir

  defp err, do: :erlang.nif_error(:nif_not_loaded)

  # Database
  def db_open, do: err()
  def db_open_path(_path), do: err()
  def db_execute(_db, _sql), do: err()

  # DataFrame
  def df_query(_db, _sql), do: err()
  def df_names(_df), do: err()
  def df_dtypes(_df), do: err()
  def df_n_rows(_df), do: err()
  def df_n_columns(_df), do: err()
  def df_pull(_df, _column), do: err()
  def df_from_csv(_db, _path), do: err()
  def df_from_parquet(_db, _path), do: err()
  def df_to_csv(_db, _df, _path), do: err()
  def df_to_parquet(_db, _df, _path), do: err()
  def df_to_rows(_df), do: err()
  def df_register_table(_db, _df, _table_name), do: err()
  def df_register_table_rc(_db, _df, _table_name), do: err()
  def df_ensure_table(_db, _df), do: err()
  def df_table_names(_db, _table_name), do: err()
  def df_dump_ipc(_df), do: err()
  def df_load_ipc(_binary), do: err()
  def df_dump_ipc_stream(_df), do: err()
  def df_load_ipc_stream(_binary), do: err()
  def df_query_stream_init(_db, _sql), do: err()
  def df_query_stream_next(_stream), do: err()
  def df_cached_table_name(_df), do: err()
  def df_from_series(_names, _series_list), do: err()

  # Series
  def s_dtype(_series), do: err()
  def s_name(_series), do: err()
  def s_size(_series), do: err()
  def s_from_list_s64(_name, _list), do: err()
  def s_from_list_s32(_name, _list), do: err()
  def s_from_list_s16(_name, _list), do: err()
  def s_from_list_s8(_name, _list), do: err()
  def s_from_list_u64(_name, _list), do: err()
  def s_from_list_u32(_name, _list), do: err()
  def s_from_list_u16(_name, _list), do: err()
  def s_from_list_u8(_name, _list), do: err()
  def s_from_list_f64(_name, _list), do: err()
  def s_from_list_f32(_name, _list), do: err()
  def s_from_list_bool(_name, _list), do: err()
  def s_from_list_str(_name, _list), do: err()
  def s_to_list(_series), do: err()
  def s_aggregate_scalar(_db, _series, _agg_fn), do: err()
  def s_binary_op(_db, _left, _right, _op), do: err()
  def s_binary_op_scalar_rhs_f64(_db, _series, _op, _scalar), do: err()
  def s_binary_op_scalar_rhs_i64(_db, _series, _op, _scalar), do: err()
  def s_sort(_db, _series, _descending, _nulls_last), do: err()
  def s_reverse(_series), do: err()
  def s_is_nil(_series), do: err()
  def s_is_not_nil(_series), do: err()
  def s_slice(_series, _offset, _length), do: err()
  def s_at(_series, _idx), do: err()
  def s_to_dataframe(_series), do: err()
  def df_from_arrow_stream_pointer(_ptr), do: err()
  def df_register_table_arrow(_db, _df, _table_name), do: err()
end
