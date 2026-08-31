# silodb: notes from a first real consumer

Written while building the eggseer cloud dashboard against silodb. Everything
below was hit in practice, not read off the spec. Ranked by how much it hurt.

**The workload, for calibration.** Egg counting, one row per egg. ~93,000
rows/day/house, 24 cameras/house, 12 houses = **~1.1M rows/day** fleet-wide,
kept for years. Each house has a local store; one cloud instance holds the
fleet. Queries are always "sum over a time bucket, grouped by camera or grid
position" for a dashboard, plus occasional ad-hoc SQL.

---

## 1. Rollup measures are REAL-only, so you cannot roll up a count

`crates/silodb/src/lib.rs:913`

```rust
} else if c.ty == silodb_schema::SqliteType::Real {
    aggs.push(quote_ident(&c.name));   // gets _count/_sum/_sumsq/_min/_max
} else {
    group.push(quote_ident(&c.name));  // becomes series identity
}
```

Counting events is *the* IoT measurement, and its natural type is INTEGER.
Our net-egg number is `sum(direction)` where direction is `+1`/`-1`, written
as an integer field. On a rollup, `direction` is not summed — it becomes a
grouping column. The single number the whole dashboard is built on is the one
a rollup cannot compute.

**Do not just add INTEGER to that match.** Integer columns are also how you
store identity: our table has `grid_x`, `grid_y`, `tracklet_id`. As measures
they collapse into `grid_x_sum` and the grid panel dies. Type alone cannot
tell a counter from an id — this is exactly what Influx encodes as tags vs
fields and Timescale as GROUP BY columns vs aggregates.

**Suggested:** let the rollup declare its measures, defaulting to today's
inference so existing rollups are untouched:

```rust
create_rollup(&conn, "counts", "10m", None)?;                  // REAL columns, as today
create_rollup(&conn, "counts", "10m", Some(&["direction"]))?;  // explicit
```

Then `direction_sum` per (10m bucket, camera) is the net count, exactly, and
re-aggregates to hourly and daily exactly. Same site needs the matching change
at `lib.rs:1344` (`add_column` widening a registered rollup).

## 2. Rollups are unreachable from the server

There is no `silodb_create_rollup()` SQL function — the registered set is
`silodb_{add_column, bucket, catalog, compact, config, create_table, datetime,
maintain, policy, schema, set_default_dir, set_retention, ts, vtab}` — and no
rollup route on the server (`/sql`, `/write`, `/query`, `/ping`, `/health`,
`/admin/*`).

So an HTTP-only consumer cannot create, list, drop, or introspect a rollup.
That is most of what "or: run it as a server" means to someone who isn't
writing Rust — including this dashboard, which reaches silodb only over
`POST /sql`. The server already half-knows rollups exist: `keys.rs:238` scopes
table names by a `rollup_` suffix.

Feature #1 is unusable from the server until this exists.

## 3. Truncation silently corrupts aggregates

`crates/silodb-server/src/lib.rs:63` — `SILODB_MAX_ROWS`, default 10,000,
response carries `truncated: true`.

For a row listing, "here are the first 10k" is reasonable. For a `GROUP BY`
feeding a chart it is not partial data, it is **wrong** data: the missing
buckets do not render as "unknown", they render as gaps or zeros, and the
chart looks plausible.

Real numbers: 24 cameras × 144 ten-minute buckets × 31 days = **107,136 rows**,
ten times the cap, for one ordinary "show me last month" panel. Nothing in the
response distinguishes that from a complete answer except a boolean the client
has to remember to check.

**Suggested:** treat a truncated aggregate as an error, not a flag — or make
the cap apply to result *bytes* with a cursor to continue. A silent-by-default
correctness cliff is the wrong default for a store that bills itself on
exactness elsewhere (sufficient statistics, byte-identical recompaction).

## 4. The container cannot be health-checked

`/health` exists (`lib.rs:146`), but the image ships exactly one thing:

```
$ docker run --rm --entrypoint sh ghcr.io/bherbruck/silodb:0.1.0 -c 'ls /usr/local/bin'
silodb-server
```

No curl, no wget, no nc, no busybox. So compose and k8s cannot probe the
endpoint without bolting a shell into the image, and our compose file carries
this apology instead of a healthcheck:

> No healthcheck: the image is debian-slim running as a non-root user with no
> curl and no wget, so there is nothing in the container to probe with.

**Suggested:** `silodb-server --healthcheck` that GETs its own `/health` and
exits 0/1, plus a `HEALTHCHECK` line in the Dockerfile. Half a day, unblocks
every orchestrator.

## 5. No edge→cloud replication — the biggest structural gap

The pitch is "edge devices, offline-first, hold data locally". The shape that
always follows is: many edge stores, one cloud aggregate. silodb has nothing
for the hop.

This is frustrating because **the cold tier is already the right artifact**.
Closed buckets are immutable Parquet with recomputable names. Shipping them
upstream is nearly a solved problem by construction: upload on close, and a
cloud instance mounts many houses' files under one table with a `house` column.

Because it doesn't exist, eggseer re-implements it as a separate service
(NATS → counts-ingest → cloud silodb over line protocol), which means the cloud
copy is a *re-derived* stream rather than the same bytes, with its own failure
modes and no way to verify the two agree.

If you build one large thing next, build this.

## 6. No server observability

No `/metrics`, no query log, no slow-query surface. On an edge box the things
you want at 2am are: write rate, last successful `maintain`, compaction lag,
hot-table row count, parquet file count, bytes on disk. Today the only way in
is `silodb_catalog` / `silodb_policy` over SQL, which requires knowing to ask.

A Prometheus endpoint is the cheap version. The valuable version is compaction
lag, because that is the failure that quietly eats a disk.

## 7. No request timeout or cancellation

No timeout layer on the server, and a client disconnect doesn't cancel the
work. Our dashboard passes an `AbortSignal` on every panel query — switching
houses or nudging the date range aborts in-flight requests — and every one of
those keeps a SQLite connection busy to completion anyway. On a single-writer
store, an impatient user is a self-inflicted stall.

## 8. Retention + downsampling is manual two-table wiring

The spec's "2y raw, 10y hourlies" is two tables, each with its own tiers and
retention, wired by hand, plus knowing to `init_table_tiered` the rollup target
*before* `create_rollup`.

That is the single most common IoT policy there is. It should be declarative —
one call that says keep raw for N, keep this grain for M, and drop raw on
schedule.

## 9. Line-protocol type inference is a footgun

Tags become TEXT, fields become typed. The same logical column therefore lands
as TEXT or INTEGER depending on how the writer framed it, with no complaint.
Our `direction` is TEXT in one store and INTEGER from the real ingester, and
the dashboard carries `CAST(direction AS INTEGER)` in every query to tolerate
both. That cast is also what hid problem #1 from us for a while.

**Suggested:** reject a write whose inferred type conflicts with an existing
column, with an error naming both types — or offer a declared-schema mode for
`/write` so autoschema is opt-in rather than the only option.

## 10. Published tags need a compatibility contract

ghcr now has `0.1.0`, `0.1`, `latest`. For a store that owns history, the tag
is a promise about data, not just a binary: does 0.2 read 0.1's parquet and
catalog? Is there a migration? What happens if an old binary opens a newer
catalog?

We pinned `0.1.0` in compose specifically because we could not answer that.
Write the answer down, and consider refusing to open a catalog written by a
newer version rather than doing something surprising.

## 11. The loadable extension gates every non-Rust consumer

`silodb-loadable` is a stub, deferred. Until it exists, "use it from Python /
Node / the sqlite3 CLI" means running the server, which means a network hop and
a token for what the pitch describes as an embedded library. The SQL admin
surface is also waiting on it, which is what makes #2 bite.

## 12. No backup story beyond "copy the volume"

hot.db plus its `.silodb/` parquet directory have to be captured consistently.
Right now that means stopping writes or getting lucky. A `silodb backup <dir>`
that takes a consistent point-in-time snapshot — SQLite backup API for hot,
hardlink-or-copy for the immutable files, which are immutable and therefore
trivially safe — would be a small command with a large trust payoff.

## 13. #1 again, in the stats table — and this one costs storage

`crates/silodb-compact/src/lib.rs:1120`

```rust
if i == ts_idx { continue }
else if c.ty == SqliteType::Real { agg_idxs.push(i) }  // aggregate
else { group_idxs.push(i) }                            // series identity
```

Same rule as #1, different consequence. In a rollup, an INTEGER measure means
you cannot compute the number. In `_stats`, it means every distinct value of
that column becomes a group — and `tracklet_id` is unique per row.

Measured on the production cloud instance after six days:

| | |
|---|---|
| `counts_stats` | **572 MB** |
| `counts_stats_path` (its index) | **258 MB** |
| `counts_hot` | 72 MB |
| all parquet, 6 days | **67 MB** |
| whole database | 962 MB |

The statistics are **830 MB of a 962 MB database** — 86% — describing 67 MB of
Parquet. Per file, stats rows against the observations they summarise:

    873,579 stats rows / 987,658 observations
    847,576 / 1,018,343
    696,088 / 1,001,809

About 0.85 stats rows per row. `quality_count` is `1` in most of them.

The comment above `FileStatsPlan` says stats buy "series-aware file pruning"
and "free whole-file aggregates — an aggregate that fully covers a chunk is one
stats-row read, no parquet". Neither works at count=1: pruning never excludes
anything, and the "free" aggregate is a full scan of a table larger than the
data. It is pure cost.

**Suggested, in order of how little the user has to know:**

1. **A cardinality guard.** At compaction, if the stats rows for a file
   approach its row count, the stats cannot prune and are not worth their
   storage — skip them and record that they were skipped. This needs no API
   change, no schema change, and no user knowledge, and it would have stopped
   this silently.
2. **Declared series identity** — `init_table_tiered(..., series = "farm,
   house, device_id")`, or a marker in the schema string. Type-based inference
   is the root cause of both #1 and this; a declaration fixes both properly.

The guard is worth having even after the declaration exists: it is the thing
that catches the case nobody declared.

## 14. The server never checkpoints the WAL, and holds readers that stop it

`crates/silodb-server/src/lib.rs` — `maintenance_loop` calls `maintain()` and
nothing else. `db.rs` spawns one writer plus a pool of read-only connections
that live for the process's lifetime.

SQLite's automatic checkpoint is passive: it copies WAL frames into the main
database, but can only reset the file when no reader holds a snapshot. With
four readers parked permanently on their own threads and a dashboard polling,
it copies and never resets.

Measured on the same instance:

    silodb.db       962 MB
    silodb.db-wal   293 MB     ← default wal_autocheckpoint is 1000 pages, ~4 MB

`silodb-bench` does `wal_checkpoint TRUNCATE`. The server never does.

**Suggested:** a `wal_checkpoint(TRUNCATE)` after each `maintain()` — it is one
line in the loop that already exists, and it bounds the WAL for every
deployment without anyone configuring anything. If TRUNCATE is too aggressive
while readers are active, PASSIVE-then-TRUNCATE on a longer interval still
beats never.

## 15. Autoschema has one branch, and it is "hypertable"

`crates/silodb-server/src/lib.rs:749` — `ensure_schema` ends in
`init_table_tiered` for any measurement that does not exist yet. There is no
way for a `/write` to land in an ordinary table.

We had a table of *current state* — what each camera in a house is called and
where it sits, about a dozen rows, rewritten only when someone renames a
camera. It reached silodb through `/write` because that was the endpoint we
already had wired, and it silently became a hypertable: daily Parquet files, a
`device_configs_stats` table, a retention policy, a catalog entry. None of that
was ever a decision, and none of it is wanted. It is not a time series.

This is the other half of #9. That one asks for declared schema so types stop
being guessed; this one asks that **tiering** stop being assumed. A store whose
whole pitch is "SQLite where data is born" should let a plain table be a plain
table.

**Suggested:** autoschema creates an ordinary table, and tiering is always
explicit (`silodb_create_table`). Failing that, a `?tiered=false` on `/write`,
or refusing autoschema entirely for a token without DDL — which brings a
related point: because autoschema needs the DDL token, our ingest service holds
create/drop rights permanently in order to do a thing that should have happened
once, at setup.

---

---

## If you only do three

1. **#1 + #2 together** — integer measures and a SQL/HTTP surface to declare a
   rollup. Individually neither is usable; together they turn a 33M-row scan
   into a 1.2M-row one for this dashboard.
2. **#3** — a truncated aggregate should not look like a complete one.
3. **#4** — a healthcheck flag, so the thing can be operated.

#5 is the one that decides whether silodb is an edge database or an IoT
platform, but it is a project, not a fix.

## Added later, after six days in production

#13 and #14 are different in kind from the rest of this list: they cost real
money on a running deployment and neither is visible from the API. The database
grew ~280 MB/day, of which ~230 MB was metadata — the stats table and its index
— against ~11 MB/day of actual Parquet. Container memory sat at 1.07 GB, about
half of it page cache over a database that should have been an eighth its size.

Both have fixes that need **no API change and no user knowledge**:

1. **#14** — `wal_checkpoint(TRUNCATE)` after `maintain()`. One line, bounds
   the WAL everywhere.
2. **#13** — a cardinality guard on `_stats`. Stops the 86%-metadata case
   silently, for anyone who ever writes an id column.

Those two are worth more than anything else on this list *for existing
deployments*, because they need nobody to know they exist. #13's declaration
half and #15 are the proper fixes and can follow.

A last one, not a bug: `retain_us` defaults to NULL and nothing ever says so.
Ours had been infinite since day one and the only symptom was a disk graph
that never went down. A line in the startup banner — how many tiered tables,
how many without retention — would have said it out loud.
