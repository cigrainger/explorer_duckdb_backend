require Explorer.DataFrame
Explorer.Backend.put(ExplorerDuckDB)

IO.puts("=== Explorer DuckDB Backend Benchmark ===\n")

# ── DataFrame Creation ──
IO.puts("── DataFrame Creation ──")
for n <- [100, 1_000, 10_000, 100_000] do
  {us, _} = :timer.tc(fn ->
    Explorer.DataFrame.new(x: Enum.to_list(1..n), y: Enum.map(1..n, &(&1 * 2)))
  end)
  IO.puts("  #{String.pad_leading(Integer.to_string(n), 7)} rows x 2 cols: #{div(us, 1000)}ms")
end

# ── Lazy Pipeline (filter + sort + head) ──
IO.puts("\n── Lazy Pipeline (filter + sort + head) ──")
for n <- [1_000, 10_000, 100_000] do
  df = Explorer.DataFrame.new(x: Enum.to_list(1..n))

  {us, _} = :timer.tc(fn ->
    df
    |> Explorer.DataFrame.filter_with(fn row -> Explorer.Series.greater(row["x"], div(n, 2)) end)
    |> Explorer.DataFrame.sort_by(asc: x)
    |> Explorer.DataFrame.head(10)
  end)
  IO.puts("  #{String.pad_leading(Integer.to_string(n), 7)} rows: #{div(us, 1000)}ms")
end

# ── Repeated Operations (cache hit) ──
IO.puts("\n── Repeated head(5) on same DataFrame ──")
for n <- [1_000, 10_000, 100_000] do
  df = Explorer.DataFrame.new(x: Enum.to_list(1..n))

  times = for _ <- 1..5 do
    {us, _} = :timer.tc(fn -> Explorer.DataFrame.head(df, 5) end)
    div(us, 1000)
  end
  IO.puts("  #{String.pad_leading(Integer.to_string(n), 7)} rows: #{Enum.join(times, ", ")}ms")
end

# ── Series Operations ──
IO.puts("\n── Series Aggregation ──")
for n <- [1_000, 10_000, 100_000] do
  s = Explorer.Series.from_list(Enum.to_list(1..n))

  {us, _} = :timer.tc(fn ->
    Explorer.Series.sum(s)
    Explorer.Series.mean(s)
    Explorer.Series.min(s)
    Explorer.Series.max(s)
  end)
  IO.puts("  #{String.pad_leading(Integer.to_string(n), 7)} elements (sum+mean+min+max): #{div(us, 1000)}ms")
end

# ── Series SQL Composition ──
IO.puts("\n── Series Chained Transforms (abs |> round |> clip) ──")
for n <- [1_000, 10_000, 100_000] do
  s = Explorer.Series.from_list(Enum.map(1..n, &(&1 * -1.5)))

  {us, _} = :timer.tc(fn ->
    s
    |> Explorer.Series.abs()
    |> Explorer.Series.round(0)
    |> Explorer.Series.clip(0, 1000)
    |> Explorer.Series.to_list()
  end)
  IO.puts("  #{String.pad_leading(Integer.to_string(n), 7)} elements: #{div(us, 1000)}ms")
end

# ── IO Round-trip ──
IO.puts("\n── IO Round-trip (dump + load) ──")
df = Explorer.DataFrame.new(x: Enum.to_list(1..10_000), y: Enum.map(1..10_000, &to_string/1))

for format <- [:csv, :parquet, :ipc, :ipc_stream] do
  dump_fn = :"dump_#{format}"
  load_fn = :"load_#{format}"

  {us, _} = :timer.tc(fn ->
    {:ok, binary} = apply(Explorer.DataFrame, dump_fn, [df])
    {:ok, _loaded} = apply(Explorer.DataFrame, load_fn, [binary])
  end)
  IO.puts("  #{format}: #{div(us, 1000)}ms")
end

# ── SQL Passthrough ──
IO.puts("\n── Raw SQL ──")
{us, _} = :timer.tc(fn ->
  ExplorerDuckDB.query("SELECT i, i * 2 AS doubled FROM generate_series(1, 100000) t(i) WHERE i > 50000 ORDER BY i DESC LIMIT 10")
end)
IO.puts("  100k row query with filter + sort + limit: #{div(us, 1000)}ms")

IO.puts("\n=== Done ===")
