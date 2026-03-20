use duckdb::Connection;
use rustler::{Encoder, Env, Error as NifError, Resource, ResourceArc, Term};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use crate::error::DuckDBExError;

/// Global connection ID counter
static CONN_ID_COUNTER: AtomicU64 = AtomicU64::new(1);

/// Wraps a DuckDB connection. DuckDB connections are not thread-safe,
/// so we protect them with a Mutex. Each connection has a unique ID
/// for table namespace isolation.
pub struct ExDuckDBRef {
    pub conn: Mutex<Connection>,
    pub id: u64,
}

#[rustler::resource_impl]
impl Resource for ExDuckDBRef {}

pub struct ExDuckDB {
    pub resource: ResourceArc<ExDuckDBRef>,
}

impl ExDuckDB {
    pub fn new(conn: Connection) -> Self {
        let id = CONN_ID_COUNTER.fetch_add(1, Ordering::Relaxed);
        Self {
            resource: ResourceArc::new(ExDuckDBRef {
                conn: Mutex::new(conn),
                id,
            }),
        }
    }

    pub fn conn_id(&self) -> u64 {
        self.resource.id
    }
}

impl Encoder for ExDuckDB {
    fn encode<'a>(&self, env: Env<'a>) -> Term<'a> {
        self.resource.encode(env)
    }
}

impl<'a> rustler::Decoder<'a> for ExDuckDB {
    fn decode(term: Term<'a>) -> rustler::NifResult<Self> {
        let resource: ResourceArc<ExDuckDBRef> = term.decode()?;
        Ok(ExDuckDB { resource })
    }
}

/// Open an in-memory DuckDB database.
#[rustler::nif(schedule = "DirtyCpu")]
fn db_open() -> Result<ExDuckDB, NifError> {
    let conn = Connection::open_in_memory()
        .map_err(|e| DuckDBExError::DuckDB(e))?;
    Ok(ExDuckDB::new(conn))
}

/// Open a DuckDB database at the given path.
#[rustler::nif(schedule = "DirtyCpu")]
fn db_open_path(path: String) -> Result<ExDuckDB, NifError> {
    let conn = Connection::open(&path)
        .map_err(|e| DuckDBExError::DuckDB(e))?;
    Ok(ExDuckDB::new(conn))
}

/// Execute a SQL statement that returns no results.
#[rustler::nif(schedule = "DirtyCpu")]
fn db_execute(db: ExDuckDB, sql: String) -> Result<(), NifError> {
    let conn = db
        .resource
        .conn
        .lock()
        .map_err(|e| DuckDBExError::Other(format!("lock error: {e}")))?;
    conn.execute_batch(&sql)
        .map_err(|e| DuckDBExError::DuckDB(e))?;
    Ok(())
}
