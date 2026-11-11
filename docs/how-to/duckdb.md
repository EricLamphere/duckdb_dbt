# Working with DuckDB

This project's warehouse is [DuckDB](https://duckdb.org): an in-process analytical database that keeps everything in one file (`dev.duckdb` or `prod.duckdb`). dbt talks to it through the `dbt-duckdb` adapter. This page covers the things you'll actually do with it.

## Where your tables are

Each target is a single file in the project folder. dbt-duckdb names the database after the file (`dev`) and prefixes custom schemas with `main_`:

| Layer | Schema | Example relation |
|---|---|---|
| Seeds / raw | `main_raw` | `dev.main_raw.raw_sales` |
| Bronze | `main_bronze` | `dev.main_bronze.stg_sales` |
| Silver | `main_silver` | `dev.main_silver.int_sales_enriched` |
| Gold | `main_gold` | `dev.main_gold.mart_sales_analytics` |

In model SQL always use `ref()` / `source()`, never these names. They're for ad-hoc queries.

## Querying

Three ways, from least to most setup:

1. **dbt show**, which compiles Jinja first:

   ```bash
   dbt show --select mart_sales_analytics --limit 20
   dbt show --inline "select channel, sum(net_amount_dollars) from {{ ref('int_sales_enriched') }} group by all"
   ```

2. **wickd's SQL Workspace**, which runs through dbt too, so `ref()` works and results land in a grid.

3. **The DuckDB CLI** (`brew install duckdb`):

   ```bash
   duckdb -readonly dev.duckdb
   ```

   ```sql
   show all tables;                                   -- every schema and table
   describe main_silver.int_sales_enriched;           -- columns and types
   summarize main_gold.mart_customer_360;             -- min/max/nulls/approx distinct per column
   ```

## The one-writer rule

A DuckDB file can be open read-write by **one process at a time**. If a dbt run fails with

```
IO Error: Could not set lock on file "dev.duckdb": Conflicting lock is held
```

something else has the file open: a DuckDB CLI session, a SQL client (DBeaver, DataGrip), a notebook. Close it, or open those tools read-only (`duckdb -readonly`, or `read_only=True` in Python) so they don't block dbt. Also why `dev` runs with `threads: 1`.

## Handy DuckDB SQL

DuckDB's "friendly SQL" saves a lot of typing in models and ad-hoc queries:

```sql
-- group by every non-aggregated column
select channel, category, sum(net_amount_dollars) as revenue
from {{ ref('int_sales_enriched') }}
group by all;

-- every column except a few, or with one replaced
select * exclude (email) from {{ ref('stg_customers') }};
select * replace (round(net_amount_dollars, 0) as net_amount_dollars) from {{ ref('int_sales_enriched') }};

-- latest row per key without a subquery
select *
from {{ ref('stg_inventory') }}
qualify row_number() over (partition by store_id, product_id order by snapshot_date desc) = 1;

-- dates
select date_trunc('month', order_ts) as month, strftime(order_ts, '%Y-%m') as month_label
from {{ ref('stg_sales') }};
```

## Reading and writing files directly

DuckDB can query files without loading them, which is great for checking a new extract before it becomes a seed or source:

```sql
select * from read_csv_auto('seeds/raw_sales.csv') limit 10;
select count(*) from read_parquet('exports/*.parquet');
```

And write query results out:

```sql
copy (select * from main_gold.mart_sales_analytics)
to 'mart_sales_analytics.parquet' (format parquet);
```

For files in S3 or over HTTP, load the `httpfs` extension (`install httpfs; load httpfs;`). With dbt-duckdb you can list extensions and settings in `profiles.yml` so every run has them:

```yaml
duckdb_dbt:
  outputs:
    dev:
      type: duckdb
      path: dev.duckdb
      extensions: [httpfs, parquet]
      settings:
        s3_region: us-east-1
```

## Comparing dev with prod

Both targets are plain files, so you can attach one to the other and diff them in SQL:

```sql
-- in duckdb -readonly dev.duckdb
attach 'prod.duckdb' as prod (read_only);

select 'dev' as side, count(*) from main_gold.mart_sales_analytics
union all
select 'prod', count(*) from prod.main_gold.mart_sales_analytics;
```

## Incremental models on DuckDB

[int_sales_enriched](ref:int_sales_enriched) is incremental with `incremental_strategy='delete+insert'` on `order_id`: each run deletes the incoming orders' existing rows and inserts the new versions. DuckDB doesn't need `merge`. To rebuild from scratch:

```bash
dbt build --select int_sales_enriched --full-refresh
```

## Starting over

The warehouse is disposable. Delete the file and rebuild:

```bash
rm dev.duckdb
dbt seed && dbt build
```

## Troubleshooting

| Symptom | Fix |
|---|---|
| `Conflicting lock is held` | Close other tools holding the file, or open them read-only (see above) |
| `Catalog Error: Table … does not exist` | Run `dbt seed`, or build the parent first (`dbt build --select +model`) |
| A model is slow | Check its compiled SQL in `target/compiled/`; prefix with `explain analyze` in the CLI |
| `prod.duckdb` is missing | `dbt seed --target prod && dbt build --target prod` |
