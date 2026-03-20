# ExplorerDuckDB

A DuckDB backend for the [Explorer](https://github.com/elixir-nx/explorer) data analysis library.

Explorer is Elixir's primary DataFrame library. This package provides an alternative backend powered by [DuckDB](https://duckdb.org), giving you access to DuckDB's analytical SQL engine, out-of-core processing, and native connectivity to Postgres, MySQL, S3, Parquet, and more.

## Architecture

The backend is **lazy-first**: DataFrame operations accumulate as a SQL query plan (CTEs). When results are needed, the entire plan executes as a single optimized DuckDB query. The eager API is syntactic sugar that calls `lazy() |> op() |> compute()`.

```
Explorer API  →  LazyFrame (accumulates ops)  →  QueryBuilder (CTE SQL)  →  DuckDB (executes)
                                                                                   ↓
                                                                         Arrow RecordBatch (zero-copy)
                                                                                   ↓
                                                                         Rustler NIF  →  Elixir
```

**Key optimizations:**
- Arrow vtab zero-copy table registration (≤2048 rows instant, larger chunked at 14x vs row-by-row)
- Connection-scoped refcounted temp table caching (0ms for repeated operations on the same DataFrame)
- NIF resource Drop auto-cleans temp tables on garbage collection
- Series SQL composition (chained transforms produce one query, not N temp tables)
- CTE-based query building for readable, optimizable SQL

## Installation

```elixir
def deps do
  [
    {:explorer_duckdb_backend, "~> 0.1.0"}
  ]
end
```

Requires Rust toolchain for compilation (DuckDB C++ is compiled from source via `duckdb-rs`).

### Configuration

```elixir
# config/config.exs
config :explorer, default_backend: ExplorerDuckDB

# Optional: default database path (default: :memory)
config :explorer_duckdb_backend, database: "/path/to/db.duckdb"

# Optional: auto-start a shared connection under supervision
config :explorer_duckdb_backend, connection: [path: "analytics.duckdb", name: :analytics]
```

## Usage

```elixir
require Explorer.DataFrame, as: DataFrame
require Explorer.Series, as: Series

Explorer.Backend.put(ExplorerDuckDB)

df = DataFrame.new(name: ["Alice", "Bob", "Carol"], score: [85, 92, 78])

df
|> DataFrame.filter(score > 80)
|> DataFrame.sort_by(desc: score)
|> DataFrame.select(["name", "score"])
```

### Connecting to databases

```elixir
# In-memory (default)
ExplorerDuckDB.open(:memory)

# File-based (persisted to disk)
ExplorerDuckDB.open("analytics.duckdb")

# Raw SQL
ExplorerDuckDB.execute("CREATE TABLE events AS SELECT * FROM read_parquet('events.parquet')")
df = ExplorerDuckDB.query("SELECT * FROM events WHERE date > '2024-01-01'")

# Attach Postgres
ExplorerDuckDB.install_extension("postgres")
ExplorerDuckDB.attach("postgres://user:pass@host/db", as: "pg", type: :postgres)
df = ExplorerDuckDB.query("SELECT * FROM pg.public.users")

# Read from S3
ExplorerDuckDB.install_extension("httpfs")
df = DataFrame.from_parquet!("s3://my-bucket/data.parquet")

# MotherDuck (cloud)
ExplorerDuckDB.open("md:my_database?motherduck_token=<token>")

# Automatic cleanup
ExplorerDuckDB.with_db("analytics.duckdb", fn ->
  df = DataFrame.from_csv!("data.csv")
  DataFrame.to_parquet(df, "output.parquet")
end)
```

### Shared connections

```elixir
# Start a shared connection (GenServer)
{:ok, conn} = ExplorerDuckDB.Connection.start_link(path: "shared.duckdb", name: :analytics)

# Use from any process -- connection monitors client processes
# and auto-cleans orphaned temp tables on process exit
ExplorerDuckDB.Connection.use(:analytics)
df = DataFrame.from_csv!("data.csv")

# Or query directly through the connection
{:ok, df} = ExplorerDuckDB.Connection.query(:analytics, "SELECT * FROM my_table")
```

### SQL passthrough

```elixir
df = DataFrame.new(sales: [100, 200, 300], region: ["N", "S", "N"])

DataFrame.sql(df, """
  SELECT region,
         SUM(sales) AS total,
         AVG(sales) AS average,
         RANK() OVER (ORDER BY SUM(sales) DESC) AS rank
  FROM tbl
  GROUP BY region
""", table_name: "tbl")
```

## What's implemented

**DataFrame** (54 of 66 callbacks):
- IO: CSV, Parquet, NDJSON (read/write/dump/load with options: delimiter, max_rows, columns, skip_rows)
- Table verbs: filter, mutate, sort, select, distinct, rename, head, tail, slice, drop_nil, put, nil_count
- Multi-table: join (inner/left/right/outer/cross), concat_rows, concat_columns
- Reshape: pivot_wider, pivot_longer, transpose, explode, unnest, dummies
- Stats: correlation, covariance, sample
- Groups: group_by + summarise
- SQL: raw SQL passthrough
- ADBC: from_query via Arrow C Stream Interface
- Not implemented: IPC format (10 callbacks -- DuckDB doesn't natively support Arrow IPC files), owner_import/export (2 callbacks)

**Series** (146 of 146 callbacks):
- Types: integers (s8-s64, u8-u64), floats (f32/f64), boolean, string, date, datetime, duration, binary, category, decimal
- Math: add, subtract, multiply, divide, pow, log, exp, abs, clip
- Trig: sin, cos, tan, asin, acos, atan, degrees, radians
- Comparison: equal, not_equal, greater, less, greater_equal, less_equal, all_equal, binary_and/or/in
- Aggregation: sum, mean, min, max, median, mode, variance, stddev, quantile, product, skew, correlation, covariance, argmin, argmax, nil_count, n_distinct
- String: contains, upcase, downcase, replace, strip/lstrip/rstrip, substring, split, split_into, json_decode, json_path_match, count_matches, re_contains, re_replace, re_count_matches, re_scan, re_named_captures
- Date/time: year, month, day_of_week, day_of_year, week_of_year, hour, minute, second, strptime, strftime
- Window: cumulative_sum/min/max/product/count, rolling_sum/min/max/mean/median/stddev
- EWM: ewm_mean, ewm_variance, ewm_standard_deviation
- Missing: fill_missing (forward/backward/min/max/mean/value)
- Sort: sort, argsort, reverse, distinct, unordered_distinct, n_distinct, frequencies
- Other: cast, categorise, categories, from_binary, to_iovec, sample, rank, peaks, cut, qcut, select, shift, coalesce, concat, format, mask, at_every, binary_in, transform, NaN/Infinity support

## Running tests

```bash
mix test
```

329 tests (20 property-based) including adversarial edge cases, lazy/eager boundary tests, concurrency tests, IO round-trip tests, and Arrow vtab data type coverage.

## License

MIT
