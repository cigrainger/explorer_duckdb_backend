# ExplorerDuckDB

A DuckDB backend for the [Explorer](https://github.com/elixir-nx/explorer) data analysis library.

Explorer is Elixir's primary DataFrame library. This package provides an alternative backend powered by [DuckDB](https://duckdb.org), giving you access to DuckDB's analytical SQL engine, out-of-core processing, and native connectivity to Postgres, MySQL, S3, Parquet, and more.

## Architecture

The backend is **lazy-first**: DataFrame operations accumulate as a SQL query plan. When results are needed, the entire plan executes as a single optimized DuckDB query. The eager API is syntactic sugar that calls `lazy() |> op() |> compute()`.

```
Explorer API  ->  LazyFrame (accumulates ops)  ->  QueryBuilder (generates SQL)  ->  DuckDB (executes)
                                                                                         |
                                                                                    Arrow RecordBatch
                                                                                         |
                                                                              Rustler NIF  ->  Elixir
```

## Installation

```elixir
def deps do
  [
    {:explorer_duckdb_backend, "~> 0.1.0"}
  ]
end
```

Requires Rust toolchain for compilation (the DuckDB C++ library is compiled from source via `duckdb-rs`).

## Usage

```elixir
require Explorer.DataFrame, as: DataFrame
require Explorer.Series, as: Series

# Set as default backend
Explorer.Backend.put(ExplorerDuckDB)

# Use Explorer as normal
df = DataFrame.new(name: ["Alice", "Bob", "Carol"], score: [85, 92, 78])

df
|> DataFrame.filter(score > 80)
|> DataFrame.sort_by(desc: score)
|> DataFrame.select(["name", "score"])
```

### Connecting to databases

```elixir
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
```

### Shared connections

```elixir
# Start a shared connection (GenServer)
{:ok, conn} = ExplorerDuckDB.Connection.start_link(path: "shared.duckdb", name: :analytics)

# Use from any process
ExplorerDuckDB.Connection.use(:analytics)
df = DataFrame.from_csv!("data.csv")

# Or query directly
{:ok, df} = ExplorerDuckDB.Connection.query(:analytics, "SELECT * FROM my_table")
```

### SQL passthrough

```elixir
df = DataFrame.new(sales: [100, 200, 300], region: ["N", "S", "N"])

DataFrame.sql(df, """
  SELECT region,
         SUM(sales) AS total,
         AVG(sales) AS average
  FROM tbl
  GROUP BY region
""", table_name: "tbl")
```

## What's implemented

**DataFrame** (55 of 66 callbacks):
- IO: CSV, Parquet, NDJSON (read/write/dump/load)
- Table verbs: filter, mutate, sort, select, distinct, rename, head, tail, slice, drop_nil, put, nil_count
- Multi-table: join (inner/left/right/outer/cross), concat_rows, concat_columns
- Reshape: pivot_wider, pivot_longer, transpose, explode, unnest, dummies
- Stats: correlation, covariance, sample
- Groups: group_by + summarise
- SQL: raw SQL passthrough
- Not implemented: IPC format (DuckDB doesn't natively support Arrow IPC files), ADBC/from_query

**Series** (134 of 146 callbacks):
- Types: integers (s8-s64, u8-u64), floats (f32/f64), boolean, string, date, datetime, binary
- Math: add, subtract, multiply, divide, pow, log, exp, abs, clip
- Trig: sin, cos, tan, asin, acos, atan, degrees, radians
- Comparison: equal, not_equal, greater, less, etc.
- Aggregation: sum, mean, min, max, median, mode, variance, stddev, quantile, product, skew, correlation, covariance
- String: contains, upcase, downcase, replace, strip, substring, split, regex ops
- Date/time: year, month, day_of_week, hour, minute, second, etc.
- Window: cumulative_sum/min/max/product/count, rolling_sum/min/max/mean/median/stddev
- EWM: ewm_mean, ewm_variance, ewm_standard_deviation
- Missing: fill_missing (forward/backward/min/max/mean/value)
- Sort: sort, argsort, reverse, distinct, n_distinct, frequencies
- Other: cast, from_binary, to_iovec, sample, rank, peaks, NaN/Infinity support

## Running tests

```bash
mix test
```

260 tests including property-based tests, adversarial edge cases, lazy/eager boundary tests, concurrency tests, and IO round-trip tests.

## License

MIT
