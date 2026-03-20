use duckdb::arrow::array::RecordBatch;
use duckdb::arrow::datatypes::SchemaRef;
use rustler::{Encoder, Env, Error as NifError, Resource, ResourceArc, Term};

use crate::database::ExDuckDB;
use crate::error::DuckDBExError;
use crate::series::ExDuckDBSeries;
use crate::types::arrow_dtype_to_explorer;

/// Holds a materialized DataFrame as Arrow RecordBatches.
pub struct ExDuckDBDataFrameRef {
    pub batches: Vec<RecordBatch>,
    pub schema: SchemaRef,
}

// Arrow RecordBatches are immutable; safe across unwind boundaries.
impl std::panic::RefUnwindSafe for ExDuckDBDataFrameRef {}

#[rustler::resource_impl]
impl Resource for ExDuckDBDataFrameRef {}

pub struct ExDuckDBDataFrame {
    pub resource: ResourceArc<ExDuckDBDataFrameRef>,
}

impl ExDuckDBDataFrame {
    pub fn new(batches: Vec<RecordBatch>, schema: SchemaRef) -> Self {
        Self {
            resource: ResourceArc::new(ExDuckDBDataFrameRef { batches, schema }),
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
        .0
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
        .0
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
        .0
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

/// Register a DataFrame as a named temporary table in DuckDB.
/// Adds a __rowid column to guarantee insertion order.
/// Uses Arrow vtab zero-copy for batches <= 2048 rows, falls back to
/// parameterized inserts for larger datasets (ArrowVTab limitation).
#[rustler::nif(schedule = "DirtyCpu")]
fn df_register_table(db: ExDuckDB, df: ExDuckDBDataFrame, table_name: String) -> Result<(), NifError> {
    use duckdb::vtab::arrow::ArrowVTab;
    use duckdb::vtab::arrow_recordbatch_to_query_params;

    let conn = db
        .resource
        .0
        .lock()
        .map_err(|e| DuckDBExError::Other(format!("lock error: {e}")))?;

    let schema = &df.resource.schema;

    // Drop if exists
    conn.execute_batch(&format!("DROP TABLE IF EXISTS {table_name}"))
        .map_err(DuckDBExError::DuckDB)?;

    if df.resource.batches.is_empty() {
        // Empty -- create table from schema only
        let mut col_defs: Vec<String> = vec!["__rowid BIGINT".to_string()];
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

        let all_arrays: Vec<_> = (0..schema.fields().len())
            .map(|col_idx| {
                let arrays: Vec<&dyn duckdb::arrow::array::Array> = df
                    .resource.batches.iter()
                    .map(|b| b.column(col_idx).as_ref())
                    .collect();
                duckdb::arrow::compute::concat(&arrays).unwrap()
            })
            .collect();

        let combined = RecordBatch::try_new(schema.clone(), all_arrays)
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
    } else {
        // For >2048 rows: chunk into 2048-row Arrow vtab inserts
        let _ = conn.register_table_function::<ArrowVTab>("arrow");

        // Concatenate all batches first
        let all_arrays: Vec<_> = (0..schema.fields().len())
            .map(|col_idx| {
                let arrays: Vec<&dyn duckdb::arrow::array::Array> = df
                    .resource.batches.iter()
                    .map(|b| b.column(col_idx).as_ref())
                    .collect();
                duckdb::arrow::compute::concat(&arrays).unwrap()
            })
            .collect();

        let combined = RecordBatch::try_new(schema.clone(), all_arrays)
            .map_err(|e| DuckDBExError::Other(format!("concat: {e}")))?;

        // Create the table from the first chunk
        let chunk_size = 2048;
        let first_chunk_len = std::cmp::min(chunk_size, total_rows);
        let first_chunk = combined.slice(0, first_chunk_len);

        let params = arrow_recordbatch_to_query_params(first_chunk);
        let sql = format!(
            "CREATE TEMPORARY TABLE {table_name} AS \
             SELECT ROW_NUMBER() OVER () - 1 AS __rowid, * FROM arrow($1, $2)"
        );
        conn.prepare(&sql)
            .map_err(DuckDBExError::DuckDB)?
            .execute(params)
            .map_err(DuckDBExError::DuckDB)?;

        // Insert remaining chunks
        let mut offset = chunk_size;
        while offset < total_rows {
            let len = std::cmp::min(chunk_size, total_rows - offset);
            let chunk = combined.slice(offset, len);
            let params = arrow_recordbatch_to_query_params(chunk);

            // Build column list for INSERT (excluding __rowid, we'll add it)
            let col_names: Vec<String> = schema.fields().iter()
                .map(|f| format!("\"{}\"", f.name()))
                .collect();

            let insert_sql = format!(
                "INSERT INTO {table_name} SELECT {} + __rowid AS __rowid, {} FROM arrow($1, $2)",
                offset,
                col_names.join(", ")
            );

            conn.prepare(&insert_sql)
                .map_err(DuckDBExError::DuckDB)?
                .execute(params)
                .map_err(DuckDBExError::DuckDB)?;

            offset += chunk_size;
        }
    }

    Ok(())
}

/// Get the list of column names, excluding __rowid.
#[rustler::nif(schedule = "DirtyCpu")]
fn df_table_names(db: ExDuckDB, table_name: String) -> Result<Vec<String>, NifError> {
    let conn = db
        .resource
        .0
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

/// Register a DataFrame using Arrow vtab zero-copy. May fail on certain types.
/// Returns Ok(true) if arrow vtab worked, Ok(false) if it wasn't attempted.
#[rustler::nif(schedule = "DirtyCpu")]
fn df_register_table_arrow(db: ExDuckDB, df: ExDuckDBDataFrame, table_name: String) -> Result<bool, NifError> {
    use duckdb::vtab::arrow::ArrowVTab;
    use duckdb::vtab::arrow_recordbatch_to_query_params;

    let conn = db
        .resource
        .0
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
        .0
        .lock()
        .map_err(|e| DuckDBExError::Other(format!("lock error: {e}")))?;

    let mut stmt = conn.prepare(sql).map_err(DuckDBExError::DuckDB)?;
    let arrow_result = stmt.query_arrow([]).map_err(DuckDBExError::DuckDB)?;

    let schema = arrow_result.get_schema();
    let batches: Vec<RecordBatch> = arrow_result.collect();

    Ok(ExDuckDBDataFrame::new(batches, schema))
}

/// Register a DataFrame as a temporary table by inserting row by row.
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
        .0
        .lock()
        .map_err(|e| DuckDBExError::Other(format!("lock error: {e}")))?;

    // Build CREATE TABLE from schema
    let schema = &df.resource.schema;
    let col_defs: Vec<String> = schema
        .fields()
        .iter()
        .map(|f| {
            format!(
                "\"{}\" {}",
                f.name(),
                arrow_type_to_duckdb_sql(f.data_type())
            )
        })
        .collect();

    conn.execute_batch(&format!(
        "CREATE TEMPORARY TABLE {table_name} ({})",
        col_defs.join(", ")
    ))
    .map_err(DuckDBExError::DuckDB)?;

    // Insert data using batched transactions
    if !df.resource.batches.is_empty() {
        let num_cols = schema.fields().len();
        let placeholders: Vec<String> = (1..=num_cols).map(|i| format!("${i}")).collect();
        let insert_sql = format!(
            "INSERT INTO {table_name} VALUES ({})",
            placeholders.join(", ")
        );

        conn.execute_batch("BEGIN TRANSACTION").map_err(DuckDBExError::DuckDB)?;
        let mut pending = 0usize;

        for batch in &df.resource.batches {
            for row_idx in 0..batch.num_rows() {
                let params = row_to_params(batch, row_idx);
                conn.execute(&insert_sql, duckdb::params_from_iter(params.iter()))
                    .map_err(DuckDBExError::DuckDB)?;
                pending += 1;
                if pending >= 1000 {
                    conn.execute_batch("COMMIT; BEGIN TRANSACTION")
                        .map_err(DuckDBExError::DuckDB)?;
                    pending = 0;
                }
            }
        }

        conn.execute_batch("COMMIT").map_err(DuckDBExError::DuckDB)?;
    }

    Ok(table_name)
}

/// Convert a row from a RecordBatch to a vector of DuckDB-compatible values.
fn row_to_params(
    batch: &RecordBatch,
    row_idx: usize,
) -> Vec<Box<dyn duckdb::types::ToSql>> {
    use duckdb::arrow::array::*;
    use duckdb::arrow::datatypes::DataType;

    let mut params: Vec<Box<dyn duckdb::types::ToSql>> = Vec::new();

    for col_idx in 0..batch.num_columns() {
        let col = batch.column(col_idx);
        if col.is_null(row_idx) {
            params.push(Box::new(None::<String>));
        } else {
            match col.data_type() {
                DataType::Boolean => {
                    let arr = col.as_any().downcast_ref::<BooleanArray>().unwrap();
                    params.push(Box::new(arr.value(row_idx)));
                }
                DataType::Int8 => {
                    let arr = col.as_any().downcast_ref::<Int8Array>().unwrap();
                    params.push(Box::new(arr.value(row_idx) as i32));
                }
                DataType::Int16 => {
                    let arr = col.as_any().downcast_ref::<Int16Array>().unwrap();
                    params.push(Box::new(arr.value(row_idx) as i32));
                }
                DataType::Int32 => {
                    let arr = col.as_any().downcast_ref::<Int32Array>().unwrap();
                    params.push(Box::new(arr.value(row_idx)));
                }
                DataType::Int64 => {
                    let arr = col.as_any().downcast_ref::<Int64Array>().unwrap();
                    params.push(Box::new(arr.value(row_idx)));
                }
                DataType::UInt8 => {
                    let arr = col.as_any().downcast_ref::<UInt8Array>().unwrap();
                    params.push(Box::new(arr.value(row_idx) as i32));
                }
                DataType::UInt16 => {
                    let arr = col.as_any().downcast_ref::<UInt16Array>().unwrap();
                    params.push(Box::new(arr.value(row_idx) as i32));
                }
                DataType::UInt32 => {
                    let arr = col.as_any().downcast_ref::<UInt32Array>().unwrap();
                    params.push(Box::new(arr.value(row_idx) as i64));
                }
                DataType::UInt64 => {
                    let arr = col.as_any().downcast_ref::<UInt64Array>().unwrap();
                    params.push(Box::new(arr.value(row_idx) as i64));
                }
                DataType::Float32 => {
                    let arr = col.as_any().downcast_ref::<Float32Array>().unwrap();
                    params.push(Box::new(arr.value(row_idx) as f64));
                }
                DataType::Float64 => {
                    let arr = col.as_any().downcast_ref::<Float64Array>().unwrap();
                    params.push(Box::new(arr.value(row_idx)));
                }
                DataType::Utf8 => {
                    let arr = col.as_any().downcast_ref::<StringArray>().unwrap();
                    params.push(Box::new(arr.value(row_idx).to_string()));
                }
                DataType::LargeUtf8 => {
                    let arr = col.as_any().downcast_ref::<LargeStringArray>().unwrap();
                    params.push(Box::new(arr.value(row_idx).to_string()));
                }
                DataType::Date32 => {
                    // Days since epoch -> convert to string date for DuckDB
                    let arr = col.as_any().downcast_ref::<Date32Array>().unwrap();
                    let days = arr.value(row_idx);
                    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                    let date = epoch + chrono::Duration::days(days as i64);
                    params.push(Box::new(date.format("%Y-%m-%d").to_string()));
                }
                DataType::Timestamp(unit, _) => {
                    use duckdb::arrow::datatypes::TimeUnit;
                    let val = match unit {
                        TimeUnit::Second => {
                            let arr = col.as_any().downcast_ref::<TimestampSecondArray>().unwrap();
                            arr.value(row_idx) * 1_000_000
                        }
                        TimeUnit::Millisecond => {
                            let arr = col.as_any().downcast_ref::<TimestampMillisecondArray>().unwrap();
                            arr.value(row_idx) * 1_000
                        }
                        TimeUnit::Microsecond => {
                            let arr = col.as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
                            arr.value(row_idx)
                        }
                        TimeUnit::Nanosecond => {
                            let arr = col.as_any().downcast_ref::<TimestampNanosecondArray>().unwrap();
                            arr.value(row_idx) / 1_000
                        }
                    };
                    // Store as microseconds since epoch
                    let secs = val / 1_000_000;
                    let usecs = (val % 1_000_000).unsigned_abs() as u32;
                    if let Some(dt) = chrono::DateTime::from_timestamp(secs, usecs * 1000) {
                        params.push(Box::new(dt.format("%Y-%m-%d %H:%M:%S%.6f").to_string()));
                    } else {
                        params.push(Box::new(None::<String>));
                    }
                }
                DataType::Decimal128(_, scale) => {
                    let arr = col.as_any().downcast_ref::<Decimal128Array>().unwrap();
                    let raw = arr.value(row_idx);
                    if *scale == 0 {
                        params.push(Box::new(raw as i64));
                    } else {
                        let divisor = 10_f64.powi(*scale as i32);
                        params.push(Box::new((raw as f64) / divisor));
                    }
                }
                _ => {
                    // Fallback: convert to NULL
                    params.push(Box::new(None::<String>));
                }
            }
        }
    }

    params
}

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
