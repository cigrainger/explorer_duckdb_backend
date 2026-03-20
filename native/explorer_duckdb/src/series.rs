use duckdb::arrow::array::{
    Array, ArrayRef, BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array,
    Int32Array, Int64Array, StringArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use duckdb::arrow::compute;
use duckdb::arrow::datatypes::DataType as ArrowDataType;
use rustler::{Encoder, Env, Error as NifError, Resource, ResourceArc, Term};
use std::sync::Arc;

use crate::database::ExDuckDB;
use crate::error::DuckDBExError;
use crate::types::arrow_dtype_to_explorer;

mod atoms {
    rustler::atoms! {
        nil_atom = "nil",
        nan,
        infinity,
        neg_infinity,
    }
}

/// Holds a single column of data as an Arrow array.
pub struct ExDuckDBSeriesRef {
    pub array: ArrayRef,
    pub name: String,
    pub dtype: ArrowDataType,
}

// Arrow arrays are safe to hold across unwind boundaries - they are immutable.
// Rustler requires RefUnwindSafe for NIF resources but Arc<dyn Array> doesn't impl it.
impl std::panic::RefUnwindSafe for ExDuckDBSeriesRef {}

#[rustler::resource_impl]
impl Resource for ExDuckDBSeriesRef {}

pub struct ExDuckDBSeries {
    pub resource: ResourceArc<ExDuckDBSeriesRef>,
}

impl ExDuckDBSeries {
    pub fn new(array: ArrayRef, name: String, dtype: ArrowDataType) -> Self {
        Self {
            resource: ResourceArc::new(ExDuckDBSeriesRef { array, name, dtype }),
        }
    }
}

impl Encoder for ExDuckDBSeries {
    fn encode<'a>(&self, env: Env<'a>) -> Term<'a> {
        self.resource.encode(env)
    }
}

impl<'a> rustler::Decoder<'a> for ExDuckDBSeries {
    fn decode(term: Term<'a>) -> rustler::NifResult<Self> {
        let resource: ResourceArc<ExDuckDBSeriesRef> = term.decode()?;
        Ok(ExDuckDBSeries { resource })
    }
}

/// Get the dtype of a Series as an Explorer dtype term.
#[rustler::nif]
fn s_dtype(env: Env, series: ExDuckDBSeries) -> Term {
    arrow_dtype_to_explorer(env, &series.resource.dtype)
}

/// Get the name of a Series.
#[rustler::nif]
fn s_name(series: ExDuckDBSeries) -> String {
    series.resource.name.clone()
}

/// Get the size of a Series.
#[rustler::nif]
fn s_size(series: ExDuckDBSeries) -> u64 {
    series.resource.array.len() as u64
}

/// Convert a Series to a single-column DataFrame for table registration.
#[rustler::nif]
fn s_to_dataframe(series: ExDuckDBSeries) -> Result<crate::dataframe::ExDuckDBDataFrame, NifError> {
    let schema = Arc::new(duckdb::arrow::datatypes::Schema::new(vec![
        duckdb::arrow::datatypes::Field::new("value", series.resource.dtype.clone(), true),
    ]));
    let batch = duckdb::arrow::array::RecordBatch::try_new(
        schema.clone(),
        vec![series.resource.array.clone()],
    )
    .map_err(|e| DuckDBExError::Other(format!("record batch from series: {e}")))?;

    Ok(crate::dataframe::ExDuckDBDataFrame::new(vec![batch], schema))
}

// ---- from_list constructors ----

#[rustler::nif]
fn s_from_list_s64(name: String, list: Vec<Option<i64>>) -> ExDuckDBSeries {
    let array: ArrayRef = Arc::new(Int64Array::from(list));
    ExDuckDBSeries::new(array, name, ArrowDataType::Int64)
}

#[rustler::nif]
fn s_from_list_s32(name: String, list: Vec<Option<i32>>) -> ExDuckDBSeries {
    let array: ArrayRef = Arc::new(Int32Array::from(list));
    ExDuckDBSeries::new(array, name, ArrowDataType::Int32)
}

#[rustler::nif]
fn s_from_list_s16(name: String, list: Vec<Option<i16>>) -> ExDuckDBSeries {
    let array: ArrayRef = Arc::new(Int16Array::from(list));
    ExDuckDBSeries::new(array, name, ArrowDataType::Int16)
}

#[rustler::nif]
fn s_from_list_s8(name: String, list: Vec<Option<i8>>) -> ExDuckDBSeries {
    let array: ArrayRef = Arc::new(Int8Array::from(list));
    ExDuckDBSeries::new(array, name, ArrowDataType::Int8)
}

#[rustler::nif]
fn s_from_list_u64(name: String, list: Vec<Option<u64>>) -> ExDuckDBSeries {
    let array: ArrayRef = Arc::new(UInt64Array::from(list));
    ExDuckDBSeries::new(array, name, ArrowDataType::UInt64)
}

#[rustler::nif]
fn s_from_list_u32(name: String, list: Vec<Option<u32>>) -> ExDuckDBSeries {
    let array: ArrayRef = Arc::new(UInt32Array::from(list));
    ExDuckDBSeries::new(array, name, ArrowDataType::UInt32)
}

#[rustler::nif]
fn s_from_list_u16(name: String, list: Vec<Option<u16>>) -> ExDuckDBSeries {
    let array: ArrayRef = Arc::new(UInt16Array::from(list));
    ExDuckDBSeries::new(array, name, ArrowDataType::UInt16)
}

#[rustler::nif]
fn s_from_list_u8(name: String, list: Vec<Option<u8>>) -> ExDuckDBSeries {
    let array: ArrayRef = Arc::new(UInt8Array::from(list));
    ExDuckDBSeries::new(array, name, ArrowDataType::UInt8)
}

/// Create a float64 series, handling :nan, :infinity, :neg_infinity atoms.
#[allow(unused_variables)]
#[rustler::nif]
fn s_from_list_f64<'a>(env: Env<'a>, name: String, list: Term<'a>) -> Result<ExDuckDBSeries, NifError> {
    let iter: Vec<Term<'a>> = list.decode()?;
    let mut builder = Float64Array::builder(iter.len());

    for term in &iter {
        if term.is_atom() {
            let atom: rustler::Atom = term.decode()?;
            if atom == atoms::nil_atom() {
                builder.append_null();
            } else if atom == atoms::nan() {
                builder.append_value(f64::NAN);
            } else if atom == atoms::infinity() {
                builder.append_value(f64::INFINITY);
            } else if atom == atoms::neg_infinity() {
                builder.append_value(f64::NEG_INFINITY);
            } else {
                builder.append_null();
            }
        } else {
            match term.decode::<f64>() {
                Ok(v) => builder.append_value(v),
                Err(_) => match term.decode::<i64>() {
                    Ok(v) => builder.append_value(v as f64),
                    Err(_) => builder.append_null(),
                },
            }
        }
    }

    let array: ArrayRef = Arc::new(builder.finish());
    Ok(ExDuckDBSeries::new(array, name, ArrowDataType::Float64))
}

/// Create a float32 series, handling :nan, :infinity, :neg_infinity atoms.
#[allow(unused_variables)]
#[rustler::nif]
fn s_from_list_f32<'a>(env: Env<'a>, name: String, list: Term<'a>) -> Result<ExDuckDBSeries, NifError> {
    let iter: Vec<Term<'a>> = list.decode()?;
    let mut builder = Float32Array::builder(iter.len());

    for term in &iter {
        if term.is_atom() {
            let atom: rustler::Atom = term.decode()?;
            if atom == atoms::nil_atom() {
                builder.append_null();
            } else if atom == atoms::nan() {
                builder.append_value(f32::NAN);
            } else if atom == atoms::infinity() {
                builder.append_value(f32::INFINITY);
            } else if atom == atoms::neg_infinity() {
                builder.append_value(f32::NEG_INFINITY);
            } else {
                builder.append_null();
            }
        } else {
            match term.decode::<f32>() {
                Ok(v) => builder.append_value(v),
                Err(_) => match term.decode::<i64>() {
                    Ok(v) => builder.append_value(v as f32),
                    Err(_) => builder.append_null(),
                },
            }
        }
    }

    let array: ArrayRef = Arc::new(builder.finish());
    Ok(ExDuckDBSeries::new(array, name, ArrowDataType::Float32))
}

#[rustler::nif]
fn s_from_list_bool(name: String, list: Vec<Option<bool>>) -> ExDuckDBSeries {
    let array: ArrayRef = Arc::new(BooleanArray::from(list));
    ExDuckDBSeries::new(array, name, ArrowDataType::Boolean)
}

#[rustler::nif]
fn s_from_list_str(name: String, list: Vec<Option<String>>) -> ExDuckDBSeries {
    let array: ArrayRef = Arc::new(StringArray::from(list));
    ExDuckDBSeries::new(array, name, ArrowDataType::Utf8)
}

/// Convert a Series to an Elixir list.
#[rustler::nif]
fn s_to_list<'a>(env: Env<'a>, series: ExDuckDBSeries) -> Term<'a> {
    let array = &series.resource.array;
    let len = array.len();
    let mut terms: Vec<Term<'a>> = Vec::with_capacity(len);

    for i in 0..len {
        terms.push(array_value_to_term(env, array.as_ref(), i));
    }

    terms.encode(env)
}

/// Convert a single value from an Arrow array to a Term.
pub fn array_value_to_term<'a>(env: Env<'a>, array: &dyn Array, idx: usize) -> Term<'a> {
    if array.is_null(idx) {
        atoms::nil_atom().encode(env)
    } else {
        match array.data_type() {
            ArrowDataType::Boolean => {
                let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
                arr.value(idx).encode(env)
            }
            ArrowDataType::Int8 => {
                let arr = array.as_any().downcast_ref::<Int8Array>().unwrap();
                arr.value(idx).encode(env)
            }
            ArrowDataType::Int16 => {
                let arr = array.as_any().downcast_ref::<Int16Array>().unwrap();
                arr.value(idx).encode(env)
            }
            ArrowDataType::Int32 => {
                let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
                arr.value(idx).encode(env)
            }
            ArrowDataType::Int64 => {
                let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
                arr.value(idx).encode(env)
            }
            ArrowDataType::UInt8 => {
                let arr = array.as_any().downcast_ref::<UInt8Array>().unwrap();
                arr.value(idx).encode(env)
            }
            ArrowDataType::UInt16 => {
                let arr = array.as_any().downcast_ref::<UInt16Array>().unwrap();
                arr.value(idx).encode(env)
            }
            ArrowDataType::UInt32 => {
                let arr = array.as_any().downcast_ref::<UInt32Array>().unwrap();
                arr.value(idx).encode(env)
            }
            ArrowDataType::UInt64 => {
                let arr = array.as_any().downcast_ref::<UInt64Array>().unwrap();
                arr.value(idx).encode(env)
            }
            ArrowDataType::Float32 => {
                let arr = array.as_any().downcast_ref::<Float32Array>().unwrap();
                let val = arr.value(idx);
                if val.is_nan() {
                    atoms::nan().encode(env)
                } else if val.is_infinite() {
                    if val > 0.0 {
                        atoms::infinity().encode(env)
                    } else {
                        atoms::neg_infinity().encode(env)
                    }
                } else {
                    val.encode(env)
                }
            }
            ArrowDataType::Float64 => {
                let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
                let val = arr.value(idx);
                if val.is_nan() {
                    atoms::nan().encode(env)
                } else if val.is_infinite() {
                    if val > 0.0 {
                        atoms::infinity().encode(env)
                    } else {
                        atoms::neg_infinity().encode(env)
                    }
                } else {
                    val.encode(env)
                }
            }
            ArrowDataType::Utf8 => {
                let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
                arr.value(idx).encode(env)
            }
            ArrowDataType::LargeUtf8 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<duckdb::arrow::array::LargeStringArray>()
                    .unwrap();
                arr.value(idx).encode(env)
            }
            ArrowDataType::Decimal128(_, scale) => {
                let arr = array
                    .as_any()
                    .downcast_ref::<duckdb::arrow::array::Decimal128Array>()
                    .unwrap();
                let raw = arr.value(idx);
                // Convert to f64 by dividing by 10^scale
                let scale = *scale as u32;
                if scale == 0 {
                    raw.encode(env)
                } else {
                    let divisor = 10_f64.powi(scale as i32);
                    ((raw as f64) / divisor).encode(env)
                }
            }
            ArrowDataType::Date32 => {
                // Days since epoch -> encode as integer (Explorer handles conversion)
                let arr = array
                    .as_any()
                    .downcast_ref::<duckdb::arrow::array::Date32Array>()
                    .unwrap();
                arr.value(idx).encode(env)
            }
            ArrowDataType::Date64 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<duckdb::arrow::array::Date64Array>()
                    .unwrap();
                arr.value(idx).encode(env)
            }
            ArrowDataType::Timestamp(unit, _) => {
                use duckdb::arrow::datatypes::TimeUnit;
                let val = match unit {
                    TimeUnit::Second => {
                        let arr = array.as_any().downcast_ref::<duckdb::arrow::array::TimestampSecondArray>().unwrap();
                        arr.value(idx) * 1_000_000
                    }
                    TimeUnit::Millisecond => {
                        let arr = array.as_any().downcast_ref::<duckdb::arrow::array::TimestampMillisecondArray>().unwrap();
                        arr.value(idx) * 1_000
                    }
                    TimeUnit::Microsecond => {
                        let arr = array.as_any().downcast_ref::<duckdb::arrow::array::TimestampMicrosecondArray>().unwrap();
                        arr.value(idx)
                    }
                    TimeUnit::Nanosecond => {
                        let arr = array.as_any().downcast_ref::<duckdb::arrow::array::TimestampNanosecondArray>().unwrap();
                        arr.value(idx) / 1_000
                    }
                };
                // Return as microseconds since epoch
                val.encode(env)
            }
            ArrowDataType::Duration(_) => {
                // Return duration as microseconds
                let arr = array.as_any().downcast_ref::<duckdb::arrow::array::DurationMicrosecondArray>();
                if let Some(arr) = arr {
                    arr.value(idx).encode(env)
                } else {
                    let arr = array.as_any().downcast_ref::<duckdb::arrow::array::DurationNanosecondArray>();
                    if let Some(arr) = arr {
                        (arr.value(idx) / 1000).encode(env)
                    } else {
                        0i64.encode(env)
                    }
                }
            }
            ArrowDataType::List(_) | ArrowDataType::LargeList(_) => {
                let list_arr = array.as_any().downcast_ref::<duckdb::arrow::array::ListArray>();
                if let Some(list_arr) = list_arr {
                    let inner = list_arr.value(idx);
                    let mut items: Vec<Term<'a>> = Vec::new();
                    for i in 0..inner.len() {
                        items.push(array_value_to_term(env, inner.as_ref(), i));
                    }
                    items.encode(env)
                } else {
                    // Try LargeList
                    let list_arr = array.as_any().downcast_ref::<duckdb::arrow::array::LargeListArray>().unwrap();
                    let inner = list_arr.value(idx);
                    let mut items: Vec<Term<'a>> = Vec::new();
                    for i in 0..inner.len() {
                        items.push(array_value_to_term(env, inner.as_ref(), i));
                    }
                    items.encode(env)
                }
            }
            ArrowDataType::Struct(_fields) => {
                let struct_arr = array.as_any().downcast_ref::<duckdb::arrow::array::StructArray>().unwrap();
                let mut map_entries: Vec<(Term<'a>, Term<'a>)> = Vec::new();
                for (col_idx, field) in _fields.iter().enumerate() {
                    let col = struct_arr.column(col_idx);
                    let key = field.name().as_str().encode(env);
                    let val = array_value_to_term(env, col.as_ref(), idx);
                    map_entries.push((key, val));
                }
                Term::map_from_pairs(env, &map_entries)
                    .unwrap_or_else(|_| atoms::nil_atom().encode(env))
            }
            ArrowDataType::Binary | ArrowDataType::LargeBinary => {
                let arr = array.as_any().downcast_ref::<duckdb::arrow::array::BinaryArray>();
                if let Some(arr) = arr {
                    let bytes = arr.value(idx);
                    let binary = match rustler::OwnedBinary::new(bytes.len()) {
                        Some(b) => b,
                        None => return atoms::nil_atom().encode(env),
                    };
                    let mut binary = binary;
                    binary.as_mut_slice().copy_from_slice(bytes);
                    rustler::Binary::from_owned(binary, env).encode(env)
                } else {
                    atoms::nil_atom().encode(env)
                }
            }
            _ => {
                // Fallback: encode as string representation
                format!("{array:?}").encode(env)
            }
        }
    }
}

/// Get a scalar aggregate value as a term.
#[rustler::nif(schedule = "DirtyCpu")]
fn s_aggregate_scalar<'a>(
    env: Env<'a>,
    db: ExDuckDB,
    series: ExDuckDBSeries,
    agg_fn: String,
) -> Result<Term<'a>, NifError> {
    let conn = db
        .resource
        .conn
        .lock()
        .map_err(|e| DuckDBExError::Other(format!("lock error: {e}")))?;

    let table_name = create_temp_series_table(&conn, &series)?;
    let sql = format!("SELECT {agg_fn}(\"value\") FROM {table_name}");

    let mut stmt = conn.prepare(&sql).map_err(DuckDBExError::DuckDB)?;
    let arrow_result = stmt.query_arrow([]).map_err(DuckDBExError::DuckDB)?;
    let batches: Vec<duckdb::arrow::array::RecordBatch> = arrow_result.collect();

    conn.execute_batch(&format!("DROP TABLE IF EXISTS {table_name}"))
        .map_err(DuckDBExError::DuckDB)?;

    if batches.is_empty() || batches[0].num_rows() == 0 {
        return Ok(atoms::nil_atom().encode(env));
    }

    Ok(array_value_to_term(env, batches[0].column(0).as_ref(), 0))
}

/// Apply a binary operation between two series via DuckDB SQL.
#[rustler::nif(schedule = "DirtyCpu")]
fn s_binary_op(
    db: ExDuckDB,
    left: ExDuckDBSeries,
    right: ExDuckDBSeries,
    op: String,
) -> Result<ExDuckDBSeries, NifError> {
    let conn = db
        .resource
        .conn
        .lock()
        .map_err(|e| DuckDBExError::Other(format!("lock error: {e}")))?;

    let left_table = create_temp_series_table(&conn, &left)?;
    let right_table = create_temp_series_table(&conn, &right)?;

    let sql = format!(
        "SELECT (l.\"value\" {op} r.\"value\") AS \"value\" \
         FROM (SELECT \"value\", ROW_NUMBER() OVER () AS __rn FROM {left_table}) l \
         JOIN (SELECT \"value\", ROW_NUMBER() OVER () AS __rn FROM {right_table}) r \
         ON l.__rn = r.__rn"
    );

    let mut stmt = conn.prepare(&sql).map_err(DuckDBExError::DuckDB)?;
    let arrow_result = stmt.query_arrow([]).map_err(DuckDBExError::DuckDB)?;
    let schema = arrow_result.get_schema();
    let batches: Vec<duckdb::arrow::array::RecordBatch> = arrow_result.collect();

    conn.execute_batch(&format!(
        "DROP TABLE IF EXISTS {left_table}; DROP TABLE IF EXISTS {right_table}"
    ))
    .map_err(DuckDBExError::DuckDB)?;

    let arrays: Vec<&dyn Array> = batches.iter().map(|b| b.column(0).as_ref()).collect();
    let concatenated = if arrays.is_empty() {
        return Err(DuckDBExError::Other("empty result from binary op".to_string()).into());
    } else if arrays.len() == 1 {
        batches[0].column(0).clone()
    } else {
        compute::concat(&arrays).map_err(DuckDBExError::Arrow)?
    };

    let dtype = schema.field(0).data_type().clone();
    Ok(ExDuckDBSeries::new(
        concatenated,
        left.resource.name.clone(),
        dtype,
    ))
}

/// Apply a binary operation between a series and a float scalar.
#[rustler::nif(schedule = "DirtyCpu")]
fn s_binary_op_scalar_rhs_f64(
    db: ExDuckDB,
    series: ExDuckDBSeries,
    op: String,
    scalar: f64,
) -> Result<ExDuckDBSeries, NifError> {
    let conn = db
        .resource
        .conn
        .lock()
        .map_err(|e| DuckDBExError::Other(format!("lock error: {e}")))?;

    let table_name = create_temp_series_table(&conn, &series)?;
    let sql = format!("SELECT (\"value\" {op} {scalar}) AS \"value\" FROM {table_name}");

    let mut stmt = conn.prepare(&sql).map_err(DuckDBExError::DuckDB)?;
    let arrow_result = stmt.query_arrow([]).map_err(DuckDBExError::DuckDB)?;
    let schema = arrow_result.get_schema();
    let batches: Vec<duckdb::arrow::array::RecordBatch> = arrow_result.collect();

    conn.execute_batch(&format!("DROP TABLE IF EXISTS {table_name}"))
        .map_err(DuckDBExError::DuckDB)?;

    let arrays: Vec<&dyn Array> = batches.iter().map(|b| b.column(0).as_ref()).collect();
    let concatenated = compute::concat(&arrays).map_err(DuckDBExError::Arrow)?;

    let dtype = schema.field(0).data_type().clone();
    Ok(ExDuckDBSeries::new(
        concatenated,
        series.resource.name.clone(),
        dtype,
    ))
}

/// Apply a binary operation between a series and an integer scalar.
#[rustler::nif(schedule = "DirtyCpu")]
fn s_binary_op_scalar_rhs_i64(
    db: ExDuckDB,
    series: ExDuckDBSeries,
    op: String,
    scalar: i64,
) -> Result<ExDuckDBSeries, NifError> {
    let conn = db
        .resource
        .conn
        .lock()
        .map_err(|e| DuckDBExError::Other(format!("lock error: {e}")))?;

    let table_name = create_temp_series_table(&conn, &series)?;
    let sql = format!("SELECT (\"value\" {op} {scalar}) AS \"value\" FROM {table_name}");

    let mut stmt = conn.prepare(&sql).map_err(DuckDBExError::DuckDB)?;
    let arrow_result = stmt.query_arrow([]).map_err(DuckDBExError::DuckDB)?;
    let schema = arrow_result.get_schema();
    let batches: Vec<duckdb::arrow::array::RecordBatch> = arrow_result.collect();

    conn.execute_batch(&format!("DROP TABLE IF EXISTS {table_name}"))
        .map_err(DuckDBExError::DuckDB)?;

    let arrays: Vec<&dyn Array> = batches.iter().map(|b| b.column(0).as_ref()).collect();
    let concatenated = compute::concat(&arrays).map_err(DuckDBExError::Arrow)?;

    let dtype = schema.field(0).data_type().clone();
    Ok(ExDuckDBSeries::new(
        concatenated,
        series.resource.name.clone(),
        dtype,
    ))
}

/// Sort a series.
#[rustler::nif(schedule = "DirtyCpu")]
fn s_sort(
    db: ExDuckDB,
    series: ExDuckDBSeries,
    descending: bool,
    nulls_last: bool,
) -> Result<ExDuckDBSeries, NifError> {
    let conn = db
        .resource
        .conn
        .lock()
        .map_err(|e| DuckDBExError::Other(format!("lock error: {e}")))?;

    let table_name = create_temp_series_table(&conn, &series)?;
    let dir = if descending { "DESC" } else { "ASC" };
    let nulls = if nulls_last {
        "NULLS LAST"
    } else {
        "NULLS FIRST"
    };
    let sql = format!("SELECT \"value\" FROM {table_name} ORDER BY \"value\" {dir} {nulls}");

    let mut stmt = conn.prepare(&sql).map_err(DuckDBExError::DuckDB)?;
    let arrow_result = stmt.query_arrow([]).map_err(DuckDBExError::DuckDB)?;
    let schema = arrow_result.get_schema();
    let batches: Vec<duckdb::arrow::array::RecordBatch> = arrow_result.collect();

    conn.execute_batch(&format!("DROP TABLE IF EXISTS {table_name}"))
        .map_err(DuckDBExError::DuckDB)?;

    let arrays: Vec<&dyn Array> = batches.iter().map(|b| b.column(0).as_ref()).collect();
    let concatenated = compute::concat(&arrays).map_err(DuckDBExError::Arrow)?;

    let dtype = schema.field(0).data_type().clone();
    Ok(ExDuckDBSeries::new(
        concatenated,
        series.resource.name.clone(),
        dtype,
    ))
}

/// Reverse a series.
#[rustler::nif]
fn s_reverse(series: ExDuckDBSeries) -> Result<ExDuckDBSeries, NifError> {
    let array = &series.resource.array;
    let len = array.len();

    let indices = Int64Array::from((0..len as i64).rev().collect::<Vec<_>>());
    let reversed = compute::take(array.as_ref(), &indices, None).map_err(DuckDBExError::Arrow)?;

    Ok(ExDuckDBSeries::new(
        reversed,
        series.resource.name.clone(),
        series.resource.dtype.clone(),
    ))
}

/// Check if values are null.
#[rustler::nif]
fn s_is_nil(series: ExDuckDBSeries) -> ExDuckDBSeries {
    let array = &series.resource.array;
    let len = array.len();
    let mut builder = duckdb::arrow::array::BooleanBuilder::with_capacity(len);

    for i in 0..len {
        builder.append_value(array.is_null(i));
    }

    let result: ArrayRef = Arc::new(builder.finish());
    ExDuckDBSeries::new(result, series.resource.name.clone(), ArrowDataType::Boolean)
}

/// Check if values are not null.
#[rustler::nif]
fn s_is_not_nil(series: ExDuckDBSeries) -> ExDuckDBSeries {
    let array = &series.resource.array;
    let len = array.len();
    let mut builder = duckdb::arrow::array::BooleanBuilder::with_capacity(len);

    for i in 0..len {
        builder.append_value(!array.is_null(i));
    }

    let result: ArrayRef = Arc::new(builder.finish());
    ExDuckDBSeries::new(result, series.resource.name.clone(), ArrowDataType::Boolean)
}

/// Slice a series.
#[rustler::nif]
fn s_slice(series: ExDuckDBSeries, offset: i64, length: u64) -> ExDuckDBSeries {
    let array = &series.resource.array;
    let len = array.len() as i64;

    let actual_offset = if offset < 0 {
        (len + offset).max(0) as usize
    } else {
        offset.min(len) as usize
    };

    let actual_length = length.min((len - actual_offset as i64).max(0) as u64) as usize;
    let sliced = array.slice(actual_offset, actual_length);

    ExDuckDBSeries::new(
        sliced,
        series.resource.name.clone(),
        series.resource.dtype.clone(),
    )
}

/// Get a single value at an index.
#[rustler::nif]
fn s_at<'a>(env: Env<'a>, series: ExDuckDBSeries, idx: u64) -> Term<'a> {
    let array = &series.resource.array;
    if (idx as usize) >= array.len() {
        atoms::nil_atom().encode(env)
    } else {
        array_value_to_term(env, array.as_ref(), idx as usize)
    }
}

// Internal helper: create a temporary single-column table from a series using row-by-row insert
fn create_temp_series_table(
    conn: &duckdb::Connection,
    series: &ExDuckDBSeries,
) -> Result<String, NifError> {
    use std::time::{SystemTime, UNIX_EPOCH};
    let id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let table_name = format!("__explorer_s_{id}");

    let duckdb_type = crate::dataframe::arrow_type_to_duckdb_sql(&series.resource.dtype);
    conn.execute_batch(&format!(
        "CREATE TEMPORARY TABLE {table_name} (\"value\" {duckdb_type})"
    ))
    .map_err(DuckDBExError::DuckDB)?;

    // Insert data row by row
    let array = &series.resource.array;
    for i in 0..array.len() {
        if array.is_null(i) {
            conn.execute(
                &format!("INSERT INTO {table_name} VALUES (NULL)"),
                [],
            )
            .map_err(DuckDBExError::DuckDB)?;
        } else {
            let val_sql = value_to_sql_string(array.as_ref(), i);
            conn.execute(
                &format!("INSERT INTO {table_name} VALUES ({val_sql})"),
                [],
            )
            .map_err(DuckDBExError::DuckDB)?;
        }
    }

    Ok(table_name)
}

fn value_to_sql_string(array: &dyn Array, idx: usize) -> String {
    match array.data_type() {
        ArrowDataType::Boolean => {
            let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            if arr.value(idx) { "TRUE".to_string() } else { "FALSE".to_string() }
        }
        ArrowDataType::Int8 => {
            let arr = array.as_any().downcast_ref::<Int8Array>().unwrap();
            arr.value(idx).to_string()
        }
        ArrowDataType::Int16 => {
            let arr = array.as_any().downcast_ref::<Int16Array>().unwrap();
            arr.value(idx).to_string()
        }
        ArrowDataType::Int32 => {
            let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
            arr.value(idx).to_string()
        }
        ArrowDataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
            arr.value(idx).to_string()
        }
        ArrowDataType::UInt8 => {
            let arr = array.as_any().downcast_ref::<UInt8Array>().unwrap();
            arr.value(idx).to_string()
        }
        ArrowDataType::UInt16 => {
            let arr = array.as_any().downcast_ref::<UInt16Array>().unwrap();
            arr.value(idx).to_string()
        }
        ArrowDataType::UInt32 => {
            let arr = array.as_any().downcast_ref::<UInt32Array>().unwrap();
            arr.value(idx).to_string()
        }
        ArrowDataType::UInt64 => {
            let arr = array.as_any().downcast_ref::<UInt64Array>().unwrap();
            arr.value(idx).to_string()
        }
        ArrowDataType::Float32 => {
            let arr = array.as_any().downcast_ref::<Float32Array>().unwrap();
            format!("{}", arr.value(idx))
        }
        ArrowDataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
            format!("{}", arr.value(idx))
        }
        ArrowDataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
            let val = arr.value(idx).replace('\'', "''");
            format!("'{val}'")
        }
        _ => "NULL".to_string(),
    }
}
