use rustler::Error as NifError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum DuckDBExError {
    #[error("DuckDB error: {0}")]
    DuckDB(#[from] duckdb::Error),
    #[error("Arrow error: {0}")]
    Arrow(#[from] duckdb::arrow::error::ArrowError),
    #[error("{0}")]
    Other(String),
}

impl From<DuckDBExError> for NifError {
    fn from(err: DuckDBExError) -> NifError {
        NifError::Term(Box::new(format!("{err}")))
    }
}
