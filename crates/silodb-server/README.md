# silodb-server

A standalone HTTP layer over a silodb database. **Not part of the core** —
the engine stays an embeddable library; this is one way to run it as a
service (the Dockerfile/compose.yml at the repo root build exactly this).

```
cargo run --release -p silodb-server
# or: docker compose up  (repo root)
```

## Configuration (env)

| Variable | Default | Meaning |
|---|---|---|
| `SILODB_DB` | `silodb.db` | SQLite file; parquet lands in `<db>.silodb/` |
| `SILODB_ADDR` | `0.0.0.0:8080` | listen address |
| `SILODB_READONLY_TOKEN` | *(disabled)* | bearer token for the readonly role |
| `SILODB_READWRITE_TOKEN` | *(disabled)* | bearer token for the readwrite role |
| `SILODB_DDL_TOKEN` | *(disabled)* | bearer token for the ddl role |
| `SILODB_DEFAULT_TIERS` | `1d` | policy for tables auto-created by `/write` |
| `SILODB_COLD_DIR` | *(derived)* | override the parquet base directory |
| `SILODB_MAINTAIN_SECS` | `60` | background `maintain()` interval; `0` disables |
| `SILODB_READERS` | `4` | read-only connection pool size |
| `SILODB_MAX_ROWS` | `10000` | `/sql` result cap (`"truncated": true` past it) |

At least one token must be set or the server refuses to start. Env
tokens are the unscoped **root credentials**; day-to-day clients should
hold provisioned keys instead (below).

## Provisioned keys — scoped credentials, managed over HTTP

Keys live in the database (`_silodb_server_keys` + a many-to-many
`_silodb_server_key_scopes` — inspectable with plain SQL like everything
else), store only a SHA-256 hash, and are shown exactly once:

```
curl -s localhost:8080/admin/api/keys -H "Authorization: Bearer $DDL" \
  -d '{"name": "site-a", "role": "write", "scope": ["weather"]}'
# {"secret":"sk_…","note":"store this now — …"}

curl -s localhost:8080/admin/api/keys -H "Authorization: Bearer $DDL"          # list (no secrets)
curl -s -X DELETE localhost:8080/admin/api/keys/site-a -H "Authorization: Bearer $DDL"  # revoke
```

`role` = `read` | `write` | `ddl`; `scope` = exact table names (empty =
unscoped). A scoped key touches **only its tables**, on every surface:

- `/write`: measurement outside scope → 403. A scoped **ddl** key may
  autoschema-create/evolve exactly the tables its scope names — so a
  fleet key can bootstrap its own measurement and nothing else.
- `/sql`: enforced by a per-request SQLite authorizer — reads limited to
  the scope's table family (`t`, `t_hot`, `t_stats`, rollups, grain
  views…), DML per role, and **no DDL or `silodb_*` admin functions at
  all** (schema changes go through the admin API, where scope is checked
  against the named table). Joins, views, `count(*)` shortcuts — the
  database refuses, not a query parser.
- `/query` (Grafana): `SHOW` output filtered to scope; `SELECT` on a
  foreign measurement errors. Point a Grafana datasource at a `read`
  key scoped to exactly the dashboards' tables.
- Key management itself needs an **unscoped** ddl credential — a scoped
  key can't mint itself wider access.

## Admin API — table management (the DDL front door)

```
GET  /admin/api/tables                       # policy + columns + hot/cold stats
POST /admin/api/tables                       # {"name","schema","tiers"?,"retention"?}
POST /admin/api/tables/{t}/columns           # {"coldef": "humidity REAL"}
PUT  /admin/api/tables/{t}/retention         # {"retain": "8w"} | {"retain": null}
```

ddl role required; a scoped ddl key may manage the tables in its scope.
Pre-creating tables here is how write-only fleets get "autoschema just
works": the admin shapes the table once, devices hold write keys that
can insert but never alter — schema drift from a device is a 403, not a
silent new column.

## Roles — enforced at the database, not just the route

| | readonly | readwrite | ddl |
|---|---|---|---|
| `SELECT` over `/sql` | ✓ | ✓ | ✓ |
| `INSERT`/`UPDATE`/`DELETE` on user tables | — | ✓ | ✓ |
| `/write` into existing schema | — | ✓ | ✓ |
| `/write` creating tables / new columns | — | — | ✓ |
| `CREATE`/`DROP`/`ALTER`, `PRAGMA`, `ATTACH` | — | — | ✓ |
| `silodb_*` admin functions | — | — | ✓ |
| writes to `_silodb_*` internals | — | — | ✓ |

readonly requests run on read-only SQLite connections; readwrite requests
run under a SQLite authorizer. A token can't out-privilege its role with
clever SQL — the database itself refuses.

## `POST /sql`

One statement per request, optional positional params:

```
curl -s localhost:8080/sql \
  -H "Authorization: Bearer $TOKEN" \
  -d '{"sql": "SELECT device, avg(value) FROM readings WHERE ts >= silodb_ts(?1) GROUP BY 1",
       "params": ["2026-07-01"]}'
# {"columns":["device","avg(value)"],"rows":[["boiler",21.4]],"truncated":false}
```

Everything the engine exposes works here: `silodb_ts`/`silodb_datetime`/
`silodb_bucket`, rollup views, joins against plain tables — and with the
ddl token, the admin functions (`silodb_create_table`, `silodb_add_column`,
`silodb_set_retention`, `silodb_maintain`, …).

## `POST /write` — InfluxDB line protocol, autoschema

```
curl -s 'localhost:8080/write?precision=s' \
  -H "Authorization: Bearer $DDL_TOKEN" \
  --data-binary 'weather,city=SF temp=21.5,humidity=40i 1752451200'
```

- measurement → table (auto-created with `SILODB_DEFAULT_TIERS` on first
  sight — ddl token only)
- tags → `TEXT` columns; fields → `REAL` (bare), `INTEGER` (`40i`,
  booleans), `TEXT` (quoted)
- a new tag/field on an existing measurement → `ADD COLUMN` evolution
  (ddl token only); older rows read `NULL`
- type conflicts are a 400 with the offending line, never a coercion
- `?precision=ns|us|ms|s` (default `ns`); missing timestamp = server now
- one request = one transaction: all lines land or none do

## Grafana — no plugin, no Infinity: it *is* an InfluxDB (to Grafana)

silodb-server emulates the InfluxDB 1.x query API (`/ping`, `/query`
with the InfluxQL subset Grafana emits), so **stock Grafana's core
InfluxDB datasource works as-is** — visual query builder, measurement/
tag/field autocomplete, template variables, the lot:

- Datasource type **InfluxDB**, query language **InfluxQL**, URL
  `http://silodb:8080`, any username, **password = a silodb token**
  (readonly is the right one for dashboards).
- The builder's dropdowns come from `SHOW MEASUREMENTS` / `SHOW TAG
  KEYS` / `SHOW FIELD KEYS` / `SHOW TAG VALUES`, answered from the
  engine's policy table and hot-table schema (tags = TEXT columns,
  fields = REAL/INTEGER — the same mapping `/write` autoschema uses,
  reversed).
- Panel queries translate to engine SQL — `GROUP BY time(30s)` becomes
  `silodb_bucket(...)`, so bucketing agrees with rollups by construction.
  Supported: `mean/sum/min/max/count/first/last/spread`, tag filters
  (`=`, `!=`, and Grafana's multi-value `=~ /^(a|b)$/`), `fill(null|
  none|0)`, `ORDER BY time DESC`, `LIMIT`, multi-statement `;`.
- Anything outside that subset returns a clear inline error naming what
  is supported (influx-style: HTTP 200, error in the result element).

`docker compose --profile grafana up` starts Grafana on :3000 with the
datasource already provisioned (see `grafana/provisioning/`).

## `GET /health`

```
{"status":"ok","tables":[{"table":"weather","active_files":3}]}
```

## Maintenance

The server runs `maintain(now)` on every policy-registered table every
`SILODB_MAINTAIN_SECS` — compaction, tier merges, retention, GC. It owns
the engine's one-maintainer contract; don't point a second maintainer at
the same database.
