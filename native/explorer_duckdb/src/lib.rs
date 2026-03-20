// Re-export duckdb's arrow to avoid version conflicts
pub use duckdb::arrow;

mod database;
mod dataframe;
mod error;
mod series;
mod temp_table;
mod types;

pub use database::{ExDuckDB, ExDuckDBRef};
pub use dataframe::{ExDuckDBDataFrame, ExDuckDBDataFrameRef};
pub use error::DuckDBExError;
pub use series::{ExDuckDBSeries, ExDuckDBSeriesRef};

rustler::init!("Elixir.ExplorerDuckDB.Native");
