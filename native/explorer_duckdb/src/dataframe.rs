use duckdb::arrow::array::RecordBatch;
use duckdb::arrow::datatypes::SchemaRef;
use rustler::{Encoder, Env, Error as NifError, Resource, ResourceArc, Term};

use crate::database::{ExDuckDB, ExDuckDBRef};
use crate::error::DuckDBExError;
use crate::series::ExDuckDBSeries;
use crate::types::arrow_dtype_to_explorer;

use std::sync::Mutex;

mod atoms {
    rustler::atoms! {
        ok,
        done,
    }
}

/// Cached temp table: (table_name, connection_id, connection_ref_for_drop)
type CachedTable = (String, u64, ResourceArc<ExDuckDBRef>);

/// Holds a materialized DataFrame as Arrow RecordBatches.
/// Includes a connection-scoped temp table cache with auto-cleanup on Drop.
pub struct ExDuckDBDataFrameRef {
    pub batches: Vec<RecordBatch>,
    pub schema: SchemaRef,
    cached_table: Mutex<Option<CachedTable>>,
}

// Arrow RecordBatches are immutable; safe across unwind boundaries.
impl std::panic::RefUnwindSafe for ExDuckDBDataFrameRef {}

#[rustler::resource_impl]
impl Resource for ExDuckDBDataFrameRef {}

impl Drop for ExDuckDBDataFrameRef {
    fn drop(&mut self) {
        if let Ok(guard) = self.cached_table.lock() {
            if let Some((ref table_name, _, ref db_ref)) = *guard {
                if let Ok(conn) = db_ref.conn.lock() {
                    let _ = conn.execute_batch(&format!("DROP TABLE IF EXISTS {table_name}"));
                }
            }
        }
    }
}

pub struct ExDuckDBDataFrame {
    pub resource: ResourceArc<ExDuckDBDataFrameRef>,
}

impl ExDuckDBDataFrame {
    pub fn new(batches: Vec<RecordBatch>, schema: SchemaRef) -> Self {
        Self {
            resource: ResourceArc::new(ExDuckDBDataFrameRef {
                batches,
                schema,
                cached_table: Mutex::new(None),
            }),
        }
    }
}

impl Encoder for ExDuckDBDataFrame {
    fn encode<'a>(&self, env: Env<'a>) -> Term<'a> {
        self.resource.encode(env)
    }
}

impl<'a> rustler::Decoder<'a> for ExDuckDBDataFrame {
    fn decode(term: Term<'a>) -> rustler::NifResult<Self> {
        let resource: ResourceArc<ExDuckDBDataFrameRef> = term.decode()?;
        Ok(ExDuckDBDataFrame { resource })
    }
}

/// Execute a SQL query and return the results as a DataFrame (Arrow RecordBatches).
#[rustler::nif(schedule = "DirtyCpu")]
fn df_query(db: ExDuckDB, sql: String) -> Result<ExDuckDBDataFrame, NifError> {
    let conn = db
        .resource
        .conn
        .lock()
        .map_err(|e| DuckDBExError::Other(format!("lock error: {e}")))?;

    let mut stmt = conn.prepare(&sql).map_err(DuckDBExError::DuckDB)?;
    let arrow_result = stmt.query_arrow([]).map_err(DuckDBExError::DuckDB)?;

    let schema = arrow_result.get_schema();
    let batches: Vec<RecordBatch> = arrow_result.collect();

    Ok(ExDuckDBDataFrame::new(batches, schema))
}

/// Get column names from a DataFrame.
#[rustler::nif]
fn df_names(df: ExDuckDBDataFrame) -> Vec<String> {
    df.resource
        .schema
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect()
}

/// Get column dtypes from a DataFrame, as Explorer dtype terms.
#[rustler::nif]
fn df_dtypes(env: Env, df: ExDuckDBDataFrame) -> Vec<Term> {
    df.resource
        .schema
        .fields()
        .iter()
        .map(|f| arrow_dtype_to_explorer(env, f.data_type()))
        .collect()
}

/// Get the number of rows in a DataFrame.
#[rustler::nif]
fn df_n_rows(df: ExDuckDBDataFrame) -> u64 {
    df.resource
        .batches
        .iter()
        .map(|b| b.num_rows() as u64)
        .sum()
}

/// Get the number of columns in a DataFrame.
#[rustler::nif]
fn df_n_columns(df: ExDuckDBDataFrame) -> u64 {
    df.resource.schema.fields().len() as u64
}

/// Pull a single column from a DataFrame as a Series.
#[rustler::nif]
fn df_pull(df: ExDuckDBDataFrame, column: String) -> Result<ExDuckDBSeries, NifError> {
    let schema = &df.resource.schema;
    let col_idx = schema
        .index_of(&column)
        .map_err(|_| DuckDBExError::Other(format!("column not found: {column}")))?;

    let field = schema.field(col_idx);
    let arrays: Vec<_> = df
        .resource
        .batches
        .iter()
        .map(|batch| batch.column(col_idx).clone())
        .collect();

    // Concatenate all chunks into a single array
    let refs: Vec<&dyn duckdb::arrow::array::Array> = arrays.iter().map(|a| a.as_ref()).collect();
    let concatenated = duckdb::arrow::compute::concat(&refs).map_err(DuckDBExError::Arrow)?;

    Ok(ExDuckDBSeries::new(
        concatenated,
        field.name().clone(),
        field.data_type().clone(),
    ))
}

/// Create a DataFrame from a SQL query reading a CSV file.
#[rustler::nif(schedule = "DirtyCpu")]
fn df_from_csv(db: ExDuckDB, path: String) -> Result<ExDuckDBDataFrame, NifError> {
    let sql = format!("SELECT * FROM read_csv_auto('{}')", path.replace('\'', "''"));
    df_query_inner(&db, &sql)
}

/// Create a DataFrame from a SQL query reading a Parquet file.
#[rustler::nif(schedule = "DirtyCpu")]
fn df_from_parquet(db: ExDuckDB, path: String) -> Result<ExDuckDBDataFrame, NifError> {
    let sql = format!(
        "SELECT * FROM read_parquet('{}')",
        path.replace('\'', "''")
    );
    df_query_inner(&db, &sql)
}

/// Write a DataFrame to CSV by inserting data into a temp table and using COPY.
#[rustler::nif(schedule = "DirtyCpu")]
fn df_to_csv(db: ExDuckDB, df: ExDuckDBDataFrame, path: String) -> Result<(), NifError> {
    let table_name = register_temp_table(&db, &df)?;
    let sql = format!(
        "COPY {table_name} TO '{}' (FORMAT CSV, HEADER)",
        path.replace('\'', "''")
    );
    let conn = db
        .resource
        .conn
        .lock()
        .map_err(|e| DuckDBExError::Other(format!("lock error: {e}")))?;
    conn.execute_batch(&sql).map_err(DuckDBExError::DuckDB)?;
    conn.execute_batch(&format!("DROP TABLE IF EXISTS {table_name}"))
        .map_err(DuckDBExError::DuckDB)?;
    Ok(())
}

/// Write a DataFrame to Parquet.
#[rustler::nif(schedule = "DirtyCpu")]
fn df_to_parquet(db: ExDuckDB, df: ExDuckDBDataFrame, path: String) -> Result<(), NifError> {
    let table_name = register_temp_table(&db, &df)?;
    let sql = format!(
        "COPY {table_name} TO '{}' (FORMAT PARQUET)",
        path.replace('\'', "''")
    );
    let conn = db
        .resource
        .conn
        .lock()
        .map_err(|e| DuckDBExError::Other(format!("lock error: {e}")))?;
    conn.execute_batch(&sql).map_err(DuckDBExError::DuckDB)?;
    conn.execute_batch(&format!("DROP TABLE IF EXISTS {table_name}"))
        .map_err(DuckDBExError::DuckDB)?;
    Ok(())
}

/// Convert a DataFrame to a list of rows (list of maps).
#[rustler::nif]
fn df_to_rows<'a>(env: Env<'a>, df: ExDuckDBDataFrame) -> Result<Term<'a>, NifError> {
    let schema = &df.resource.schema;
    let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();

    let mut rows: Vec<Term<'a>> = Vec::new();

    for batch in &df.resource.batches {
        for row_idx in 0..batch.num_rows() {
            let mut map_entries: Vec<(Term<'a>, Term<'a>)> = Vec::new();
            for (col_idx, name) in names.iter().enumerate() {
                let col = batch.column(col_idx);
                let value = crate::series::array_value_to_term(env, col.as_ref(), row_idx);
                map_entries.push((name.encode(env), value));
            }
            let map = Term::map_from_pairs(env, &map_entries)
                .map_err(|_| DuckDBExError::Other("failed to create map".to_string()))?;
            rows.push(map);
        }
    }

    Ok(rows.encode(env))
}

/// Ensure a DataFrame has a registered temp table. Returns the table name.
/// If already cached for this connection, returns instantly.
/// The table is auto-dropped when the DataFrame NIF resource is GC'd.
#[rustler::nif(schedule = "DirtyCpu")]
fn df_ensure_table(db: ExDuckDB, df: ExDuckDBDataFrame) -> Result<String, NifError> {
    let conn_id = db.conn_id();

    // Check cache -- only reuse if same connection
    if let Ok(guard) = df.resource.cached_table.lock() {
        if let Some((ref name, ref cached_conn_id, _)) = *guard {
            if *cached_conn_id == conn_id {
                return Ok(name.clone());
            }
        }
    }

    // Generate table name scoped to connection
    let table_id = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let table_name = format!("__explorer_c{conn_id}_{table_id}");

    df_register_table_impl(&db, &df, &table_name)?;

    // Cache it
    if let Ok(mut guard) = df.resource.cached_table.lock() {
        *guard = Some((table_name.clone(), conn_id, db.resource.clone()));
    }

    Ok(table_name)
}

/// Register a DataFrame as a refcounted temporary table (legacy).
#[rustler::nif(schedule = "DirtyCpu")]
fn df_register_table_rc(db: ExDuckDB, df: ExDuckDBDataFrame, table_name: String) -> Result<crate::temp_table::ExTempTable, NifError> {
    df_register_table_impl(&db, &df, &table_name)?;
    Ok(crate::temp_table::ExTempTable::new(table_name, db.resource.clone()))
}

/// Register a DataFrame as a named temporary table in DuckDB.
/// Adds a __rowid column to guarantee insertion order.
/// Uses Arrow vtab zero-copy for batches <= 2048 rows, falls back to
/// parameterized inserts for larger datasets (ArrowVTab limitation).
#[rustler::nif(schedule = "DirtyCpu")]
fn df_register_table(db: ExDuckDB, df: ExDuckDBDataFrame, table_name: String) -> Result<(), NifError> {
    df_register_table_impl(&db, &df, &table_name)
}

fn df_register_table_impl(db: &ExDuckDB, df: &ExDuckDBDataFrame, table_name: &str) -> Result<(), NifError> {
    use duckdb::vtab::arrow::ArrowVTab;
    use duckdb::vtab::arrow_recordbatch_to_query_params;

    let conn = db
        .resource
        .conn
        .lock()
        .map_err(|e| DuckDBExError::Other(format!("lock error: {e}")))?;

    let schema = &df.resource.schema;

    // Drop if exists
    conn.execute_batch(&format!("DROP TABLE IF EXISTS {table_name}"))
        .map_err(DuckDBExError::DuckDB)?;

    // Check if schema already has __rowid (from previous registration)
    let has_rowid = schema.fields().iter().any(|f| f.name() == "__rowid");
    let rowid_prefix = if has_rowid { "" } else { "ROW_NUMBER() OVER () - 1 AS __rowid, " };

    if df.resource.batches.is_empty() {
        // Empty -- create table from schema only
        let mut col_defs: Vec<String> = if has_rowid { vec![] } else { vec!["__rowid BIGINT".to_string()] };
        for field in schema.fields() {
            col_defs.push(format!("\"{}\" {}", field.name(), arrow_type_to_duckdb_sql(field.data_type())));
        }
        conn.execute_batch(&format!("CREATE TEMPORARY TABLE {table_name} ({})", col_defs.join(", ")))
            .map_err(DuckDBExError::DuckDB)?;
        return Ok(());
    }

    // Count total rows
    let total_rows: usize = df.resource.batches.iter().map(|b| b.num_rows()).sum();

    // ArrowVTab only supports single-batch up to 2048 rows (DuckDB vector size)
    if total_rows <= 2048 {
        // Arrow zero-copy path
        let _ = conn.register_table_function::<ArrowVTab>("arrow");

        let all_arrays: Result<Vec<_>, _> = (0..schema.fields().len())
            .map(|col_idx| {
                let arrays: Vec<&dyn duckdb::arrow::array::Array> = df
                    .resource.batches.iter()
                    .map(|b| b.column(col_idx).as_ref())
                    .collect();
                duckdb::arrow::compute::concat(&arrays)
            })
            .collect();

        let all_arrays = all_arrays
            .map_err(|e| DuckDBExError::Other(format!("concat arrays: {e}")))?;

        let combined = RecordBatch::try_new(schema.clone(), all_arrays)
            .map_err(|e| DuckDBExError::Other(format!("concat: {e}")))?;

        let params = arrow_recordbatch_to_query_params(combined);
        let sql = format!(
            "CREATE TEMPORARY TABLE {table_name} AS \
             SELECT {rowid_prefix}* FROM arrow($1, $2)"
        );
        conn.prepare(&sql)
            .map_err(DuckDBExError::DuckDB)?
            .execute(params)
            .map_err(DuckDBExError::DuckDB)?;
    } else {
        // For >2048 rows: use Appender::append_record_batch
        // The Appender handles chunking automatically (vector_size batches)
        let mut col_defs: Vec<String> = if has_rowid { vec![] } else { vec!["__rowid BIGINT".to_string()] };
        for field in schema.fields() {
            col_defs.push(format!("\"{}\" {}", field.name(), arrow_type_to_duckdb_sql(field.data_type())));
        }
        conn.execute_batch(&format!("CREATE TEMPORARY TABLE {table_name} ({})", col_defs.join(", ")))
            .map_err(DuckDBExError::DuckDB)?;

        if has_rowid {
            // Data already has __rowid, append directly
            let mut appender = conn.appender(&table_name)
                .map_err(DuckDBExError::DuckDB)?;

            for batch in &df.resource.batches {
                appender.append_record_batch(batch.clone())
                    .map_err(DuckDBExError::DuckDB)?;
            }
            appender.flush().map_err(DuckDBExError::DuckDB)?;
        } else {
            // Need to add __rowid column -- append data first, then add __rowid
            // Create temp table without __rowid, append, then ALTER to add it
            let data_table = format!("{table_name}_data");
            let mut data_col_defs: Vec<String> = Vec::new();
            for field in schema.fields() {
                data_col_defs.push(format!("\"{}\" {}", field.name(), arrow_type_to_duckdb_sql(field.data_type())));
            }
            conn.execute_batch(&format!("DROP TABLE IF EXISTS {table_name}"))
                .map_err(DuckDBExError::DuckDB)?;
            conn.execute_batch(&format!("CREATE TEMPORARY TABLE {data_table} ({})", data_col_defs.join(", ")))
                .map_err(DuckDBExError::DuckDB)?;

            let mut appender = conn.appender(&data_table)
                .map_err(DuckDBExError::DuckDB)?;

            for batch in &df.resource.batches {
                appender.append_record_batch(batch.clone())
                    .map_err(DuckDBExError::DuckDB)?;
            }
            appender.flush().map_err(DuckDBExError::DuckDB)?;
            drop(appender);

            // Create final table with __rowid from the data table
            let col_list: Vec<String> = schema.fields().iter()
                .map(|f| format!("\"{}\"", f.name()))
                .collect();
            conn.execute_batch(&format!(
                "CREATE TEMPORARY TABLE {table_name} AS \
                 SELECT ROW_NUMBER() OVER () - 1 AS __rowid, {} FROM {data_table}",
                col_list.join(", ")
            ))
            .map_err(DuckDBExError::DuckDB)?;

            conn.execute_batch(&format!("DROP TABLE IF EXISTS {data_table}"))
                .map_err(DuckDBExError::DuckDB)?;
        }
    }

    Ok(())
}

/// Get the list of column names, excluding __rowid.
#[rustler::nif(schedule = "DirtyCpu")]
fn df_table_names(db: ExDuckDB, table_name: String) -> Result<Vec<String>, NifError> {
    let conn = db
        .resource
        .conn
        .lock()
        .map_err(|e| DuckDBExError::Other(format!("lock error: {e}")))?;

    let stmt = conn
        .prepare(&format!("SELECT * FROM {table_name} LIMIT 0"))
        .map_err(DuckDBExError::DuckDB)?;
    let cols = stmt.column_names();
    Ok(cols.into_iter().filter(|c| c != "__rowid").collect())
}

/// Create a DataFrame from an Arrow C Stream pointer (used by ADBC).
#[rustler::nif(schedule = "DirtyCpu")]
fn df_from_arrow_stream_pointer(stream_ptr: u64) -> Result<ExDuckDBDataFrame, NifError> {
    use duckdb::arrow::array::RecordBatchReader;
    use duckdb::arrow::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};

    let ptr = stream_ptr as *mut FFI_ArrowArrayStream;
    let stream = unsafe { std::ptr::read(ptr) };

    let reader = ArrowArrayStreamReader::try_new(stream)
        .map_err(|e| DuckDBExError::Other(format!("arrow stream error: {e}")))?;

    let schema = reader.schema();
    let batches: Vec<RecordBatch> = reader
        .filter_map(|r| r.ok())
        .collect();

    Ok(ExDuckDBDataFrame::new(batches, schema))
}

// ============================================================
// Arrow IPC format support
// ============================================================

/// Serialize a DataFrame to Arrow IPC file format (random-access).
#[rustler::nif(schedule = "DirtyCpu")]
fn df_dump_ipc<'a>(env: Env<'a>, df: ExDuckDBDataFrame) -> Result<rustler::Binary<'a>, NifError> {
    use arrow::ipc::writer::FileWriter;

    let mut buf = Vec::new();
    {
        let schema = &*df.resource.schema;
        let mut writer = FileWriter::try_new(&mut buf, schema)
            .map_err(|e| DuckDBExError::Other(format!("IPC write error: {e}")))?;

        for batch in &df.resource.batches {
            writer.write(batch)
                .map_err(|e| DuckDBExError::Other(format!("IPC write error: {e}")))?;
        }
        writer.finish()
            .map_err(|e| DuckDBExError::Other(format!("IPC finish error: {e}")))?;
    }

    let mut binary = rustler::OwnedBinary::new(buf.len())
        .ok_or_else(|| DuckDBExError::Other("failed to allocate binary".to_string()))?;
    binary.as_mut_slice().copy_from_slice(&buf);
    Ok(rustler::Binary::from_owned(binary, env))
}

/// Deserialize a DataFrame from Arrow IPC file format.
#[rustler::nif(schedule = "DirtyCpu")]
fn df_load_ipc(binary: rustler::Binary) -> Result<ExDuckDBDataFrame, NifError> {
    use arrow::ipc::reader::FileReader;
    use std::io::Cursor;

    let cursor = Cursor::new(binary.as_slice());
    let reader = FileReader::try_new(cursor, None)
        .map_err(|e| DuckDBExError::Other(format!("IPC read error: {e}")))?;

    let schema = reader.schema();
    let batches: Vec<RecordBatch> = reader
        .filter_map(|r| r.ok())
        .collect();

    Ok(ExDuckDBDataFrame::new(batches, schema))
}

/// Serialize a DataFrame to Arrow IPC streaming format.
#[rustler::nif(schedule = "DirtyCpu")]
fn df_dump_ipc_stream<'a>(env: Env<'a>, df: ExDuckDBDataFrame) -> Result<rustler::Binary<'a>, NifError> {
    use arrow::ipc::writer::StreamWriter;

    let mut buf = Vec::new();
    {
        let schema = &*df.resource.schema;
        let mut writer = StreamWriter::try_new(&mut buf, schema)
            .map_err(|e| DuckDBExError::Other(format!("IPC stream write error: {e}")))?;

        for batch in &df.resource.batches {
            writer.write(batch)
                .map_err(|e| DuckDBExError::Other(format!("IPC stream write error: {e}")))?;
        }
        writer.finish()
            .map_err(|e| DuckDBExError::Other(format!("IPC stream finish error: {e}")))?;
    }

    let mut binary = rustler::OwnedBinary::new(buf.len())
        .ok_or_else(|| DuckDBExError::Other("failed to allocate binary".to_string()))?;
    binary.as_mut_slice().copy_from_slice(&buf);
    Ok(rustler::Binary::from_owned(binary, env))
}

/// Deserialize a DataFrame from Arrow IPC streaming format.
#[rustler::nif(schedule = "DirtyCpu")]
fn df_load_ipc_stream(binary: rustler::Binary) -> Result<ExDuckDBDataFrame, NifError> {
    use arrow::ipc::reader::StreamReader;
    use std::io::Cursor;

    let cursor = Cursor::new(binary.as_slice());
    let reader = StreamReader::try_new(cursor, None)
        .map_err(|e| DuckDBExError::Other(format!("IPC stream read error: {e}")))?;

    let schema = reader.schema();
    let batches: Vec<RecordBatch> = reader
        .filter_map(|r| r.ok())
        .collect();

    Ok(ExDuckDBDataFrame::new(batches, schema))
}

/// Register a DataFrame using Arrow vtab zero-copy. May fail on certain types.
/// Returns Ok(true) if arrow vtab worked, Ok(false) if it wasn't attempted.
#[rustler::nif(schedule = "DirtyCpu")]
fn df_register_table_arrow(db: ExDuckDB, df: ExDuckDBDataFrame, table_name: String) -> Result<bool, NifError> {
    use duckdb::vtab::arrow::ArrowVTab;
    use duckdb::vtab::arrow_recordbatch_to_query_params;

    let conn = db
        .resource
        .conn
        .lock()
        .map_err(|e| DuckDBExError::Other(format!("lock error: {e}")))?;

    let _ = conn.register_table_function::<ArrowVTab>("arrow");

    conn.execute_batch(&format!("DROP TABLE IF EXISTS {table_name}"))
        .map_err(DuckDBExError::DuckDB)?;

    let schema = df.resource.schema.clone();

    if df.resource.batches.is_empty() {
        return Ok(false);
    }

    // Concatenate batches
    let all_arrays: Vec<_> = (0..schema.fields().len())
        .map(|col_idx| {
            let arrays: Vec<&dyn duckdb::arrow::array::Array> = df
                .resource
                .batches
                .iter()
                .map(|b| b.column(col_idx).as_ref())
                .collect();
            duckdb::arrow::compute::concat(&arrays).unwrap()
        })
        .collect();

    let combined = RecordBatch::try_new(schema, all_arrays)
        .map_err(|e| DuckDBExError::Other(format!("concat: {e}")))?;

    let params = arrow_recordbatch_to_query_params(combined);
    let sql = format!(
        "CREATE TEMPORARY TABLE {table_name} AS \
         SELECT ROW_NUMBER() OVER () - 1 AS __rowid, * FROM arrow($1, $2)"
    );

    conn.prepare(&sql)
        .map_err(DuckDBExError::DuckDB)?
        .execute(params)
        .map_err(DuckDBExError::DuckDB)?;

    Ok(true)
}

// Internal helpers

fn df_query_inner(db: &ExDuckDB, sql: &str) -> Result<ExDuckDBDataFrame, NifError> {
    let conn = db
        .resource
        .conn
        .lock()
        .map_err(|e| DuckDBExError::Other(format!("lock error: {e}")))?;

    let mut stmt = conn.prepare(sql).map_err(DuckDBExError::DuckDB)?;
    let arrow_result = stmt.query_arrow([]).map_err(DuckDBExError::DuckDB)?;

    let schema = arrow_result.get_schema();
    let batches: Vec<RecordBatch> = arrow_result.collect();

    Ok(ExDuckDBDataFrame::new(batches, schema))
}

/// Register a DataFrame as a temporary table using Appender.
/// Returns the table name.
fn register_temp_table(db: &ExDuckDB, df: &ExDuckDBDataFrame) -> Result<String, NifError> {
    use std::time::{SystemTime, UNIX_EPOCH};
    let id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let table_name = format!("__explorer_tmp_{id}");

    let conn = db
        .resource
        .conn
        .lock()
        .map_err(|e| DuckDBExError::Other(format!("lock error: {e}")))?;

    let schema = &df.resource.schema;
    let col_defs: Vec<String> = schema
        .fields()
        .iter()
        .map(|f| format!("\"{}\" {}", f.name(), arrow_type_to_duckdb_sql(f.data_type())))
        .collect();

    conn.execute_batch(&format!(
        "CREATE TEMPORARY TABLE {table_name} ({})",
        col_defs.join(", ")
    ))
    .map_err(DuckDBExError::DuckDB)?;

    if !df.resource.batches.is_empty() {
        let mut appender = conn.appender(&table_name)
            .map_err(DuckDBExError::DuckDB)?;

        for batch in &df.resource.batches {
            appender.append_record_batch(batch.clone())
                .map_err(DuckDBExError::DuckDB)?;
        }
        appender.flush().map_err(DuckDBExError::DuckDB)?;
    }

    Ok(table_name)
}

// ============================================================
// Streaming query results
// ============================================================

/// A streaming query result. Holds the Arrow iterator from DuckDB.
pub struct ExQueryStreamRef {
    batches: Mutex<Vec<RecordBatch>>,
    schema: SchemaRef,
    position: Mutex<usize>,
}

impl std::panic::RefUnwindSafe for ExQueryStreamRef {}

#[rustler::resource_impl]
impl Resource for ExQueryStreamRef {}

pub struct ExQueryStream {
    pub resource: ResourceArc<ExQueryStreamRef>,
}

impl Encoder for ExQueryStream {
    fn encode<'a>(&self, env: Env<'a>) -> Term<'a> {
        self.resource.encode(env)
    }
}

impl<'a> rustler::Decoder<'a> for ExQueryStream {
    fn decode(term: Term<'a>) -> rustler::NifResult<Self> {
        let resource: ResourceArc<ExQueryStreamRef> = term.decode()?;
        Ok(ExQueryStream { resource })
    }
}

/// Start a streaming query. Returns a stream handle.
#[rustler::nif(schedule = "DirtyCpu")]
fn df_query_stream_init(db: ExDuckDB, sql: String) -> Result<(ExQueryStream, u64), NifError> {
    let conn = db
        .resource
        .conn
        .lock()
        .map_err(|e| DuckDBExError::Other(format!("lock error: {e}")))?;

    let mut stmt = conn.prepare(&sql).map_err(DuckDBExError::DuckDB)?;
    let arrow_result = stmt.query_arrow([]).map_err(DuckDBExError::DuckDB)?;

    let schema = arrow_result.get_schema();
    let batches: Vec<RecordBatch> = arrow_result.collect();
    let total = batches.len() as u64;

    let stream = ExQueryStream {
        resource: ResourceArc::new(ExQueryStreamRef {
            batches: Mutex::new(batches),
            schema,
            position: Mutex::new(0),
        }),
    };

    Ok((stream, total))
}

/// Get the next batch from a streaming query. Returns {:ok, df} or :done.
#[rustler::nif]
fn df_query_stream_next<'a>(env: Env<'a>, stream: ExQueryStream) -> Result<Term<'a>, NifError> {
    let pos = {
        let mut pos = stream.resource.position.lock()
            .map_err(|e| DuckDBExError::Other(format!("stream lock: {e}")))?;
        let current = *pos;
        *pos += 1;
        current
    };

    let batches = stream.resource.batches.lock()
        .map_err(|e| DuckDBExError::Other(format!("stream lock: {e}")))?;

    if pos >= batches.len() {
        Ok(atoms::done().encode(env))
    } else {
        let batch = batches[pos].clone();
        let schema = stream.resource.schema.clone();
        let df = ExDuckDBDataFrame::new(vec![batch], schema);
        Ok((atoms::ok(), df).encode(env))
    }
}

// ============================================================
// Perseus-style optimization: check if DataFrame can be mutated in place
// ============================================================

/// Create a DataFrame from multiple Series (columns).
/// Much faster than creating N temp tables and joining by ROW_NUMBER.
#[rustler::nif(schedule = "DirtyCpu")]
fn df_from_series(names: Vec<String>, series_list: Vec<crate::series::ExDuckDBSeries>) -> Result<ExDuckDBDataFrame, NifError> {
    if names.len() != series_list.len() {
        return Err(DuckDBExError::Other("names and series count mismatch".to_string()).into());
    }

    if names.is_empty() {
        let schema = std::sync::Arc::new(duckdb::arrow::datatypes::Schema::empty());
        return Ok(ExDuckDBDataFrame::new(vec![], schema));
    }

    // Build schema and arrays from the series
    let fields: Vec<duckdb::arrow::datatypes::Field> = names
        .iter()
        .zip(series_list.iter())
        .map(|(name, series)| {
            duckdb::arrow::datatypes::Field::new(name, series.resource.dtype.clone(), true)
        })
        .collect();

    let schema = std::sync::Arc::new(duckdb::arrow::datatypes::Schema::new(fields));

    let arrays: Vec<duckdb::arrow::array::ArrayRef> = series_list
        .iter()
        .map(|s| s.resource.array.clone())
        .collect();

    let batch = RecordBatch::try_new(schema.clone(), arrays)
        .map_err(|e| DuckDBExError::Other(format!("from_series batch: {e}")))?;

    Ok(ExDuckDBDataFrame::new(vec![batch], schema))
}

/// Check if this DataFrame has a cached temp table.
/// Returns the table name if cached, nil otherwise.
/// Used by Elixir to determine if re-registration can be skipped.
#[rustler::nif]
fn df_cached_table_name(df: ExDuckDBDataFrame) -> Option<String> {
    df.resource
        .cached_table
        .lock()
        .ok()
        .and_then(|guard| guard.as_ref().map(|(name, _, _)| name.clone()))
}

// Note: Perseus-style in-place mutation (Arc::strong_count == 1) is not
// possible with Rustler's ResourceArc, which uses BEAM-level reference
// counting that isn't exposed to NIFs. The BEAM GC manages the refcount
// and only Drop is called when it reaches zero.
//
// To enable in-place mutation, Rustler would need to add:
//   pub fn strong_count(resource: &ResourceArc<T>) -> usize
// backed by enif_term_to_resource + reference counting inspection.
//
// For now, we use copy-on-write semantics: every compute_to_eager
// creates a new DataFrame from the query result.

pub fn arrow_type_to_duckdb_sql(dt: &duckdb::arrow::datatypes::DataType) -> String {
    match dt {
        duckdb::arrow::datatypes::DataType::Boolean => "BOOLEAN".to_string(),
        duckdb::arrow::datatypes::DataType::Int8 => "TINYINT".to_string(),
        duckdb::arrow::datatypes::DataType::Int16 => "SMALLINT".to_string(),
        duckdb::arrow::datatypes::DataType::Int32 => "INTEGER".to_string(),
        duckdb::arrow::datatypes::DataType::Int64 => "BIGINT".to_string(),
        duckdb::arrow::datatypes::DataType::UInt8 => "UTINYINT".to_string(),
        duckdb::arrow::datatypes::DataType::UInt16 => "USMALLINT".to_string(),
        duckdb::arrow::datatypes::DataType::UInt32 => "UINTEGER".to_string(),
        duckdb::arrow::datatypes::DataType::UInt64 => "UBIGINT".to_string(),
        duckdb::arrow::datatypes::DataType::Float32 => "FLOAT".to_string(),
        duckdb::arrow::datatypes::DataType::Float64 => "DOUBLE".to_string(),
        duckdb::arrow::datatypes::DataType::Utf8 | duckdb::arrow::datatypes::DataType::LargeUtf8 => {
            "VARCHAR".to_string()
        }
        duckdb::arrow::datatypes::DataType::Binary | duckdb::arrow::datatypes::DataType::LargeBinary => {
            "BLOB".to_string()
        }
        duckdb::arrow::datatypes::DataType::Date32 | duckdb::arrow::datatypes::DataType::Date64 => {
            "DATE".to_string()
        }
        duckdb::arrow::datatypes::DataType::Timestamp(_, None) => "TIMESTAMP".to_string(),
        duckdb::arrow::datatypes::DataType::Timestamp(_, Some(_)) => "TIMESTAMPTZ".to_string(),
        duckdb::arrow::datatypes::DataType::Duration(_) => "INTERVAL".to_string(),
        duckdb::arrow::datatypes::DataType::Decimal128(p, s) => format!("DECIMAL({p}, {s})"),
        duckdb::arrow::datatypes::DataType::List(field) | duckdb::arrow::datatypes::DataType::LargeList(field) => {
            format!("{}[]", arrow_type_to_duckdb_sql(field.data_type()))
        }
        duckdb::arrow::datatypes::DataType::Struct(fields) => {
            let field_defs: Vec<String> = fields
                .iter()
                .map(|f| {
                    format!(
                        "\"{}\" {}",
                        f.name(),
                        arrow_type_to_duckdb_sql(f.data_type())
                    )
                })
                .collect();
            format!("STRUCT({})", field_defs.join(", "))
        }
        duckdb::arrow::datatypes::DataType::Null => "INTEGER".to_string(),
        _ => "VARCHAR".to_string(),
    }
}
