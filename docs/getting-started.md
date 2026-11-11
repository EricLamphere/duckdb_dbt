# Getting started

Everything you need to go from a fresh clone to a fully built project in a few minutes. The whole warehouse is a DuckDB file on your laptop, so there's no account or server to set up.

## 1. Install

You need Python 3.11+.

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt   # dbt-duckdb (brings dbt-core)
dbt deps                          # dbt_utils, from packages.yml
```

In wickd, opening the project runs these for you: the init pipeline's `pip install` and `dbt deps` steps, then the scripts in `init/`.

## 2. Load the raw data

```bash
dbt seed
```

This loads the five CSVs in `seeds/` into the `main_raw` schema, where the [raw source](source:raw.raw_sales) reads them.

## 3. Build

```bash
dbt build
```

`dbt build` runs models, tests, seeds and snapshots in dependency order and stops downstream of anything that fails. Some useful variations:

| Command | Does |
|---|---|
| `dbt build --select stg_sales+` | [stg_sales](ref:stg_sales) and everything downstream |
| `dbt build --select +mart_customer_360` | [mart_customer_360](ref:mart_customer_360) and everything it needs |
| `dbt build --select bronze` | Every model in `models/bronze/` |
| `dbt build --select state:modified+ --state <prod artifacts>` | Only what you changed, and its children |
| `dbt build --full-refresh --select int_sales_enriched` | Rebuild the incremental model from scratch |
| `dbt source freshness` | Check how old the raw data is |

## 4. Targets

`profiles.yml` in this folder defines two targets, each its own DuckDB file:

| Target | File | Threads | Use |
|---|---|---|---|
| `dev` (default) | `dev.duckdb` | 1 | Your day-to-day work |
| `prod` | `prod.duckdb` | 4 | The "production" copy that slim builds and defer compare against |

Pick one with `--target prod`, or with the Target dropdown in wickd's header.

## 5. Look at the results

- **In wickd:** DAG for lineage and run status, Docs for every model's columns and tests, SQL Workspace for ad-hoc queries, Metrics for the semantic layer.
- **From the command line:** `dbt show --select mart_sales_analytics --limit 10`, or open the file directly. See [Working with DuckDB](how-to/duckdb.md).

## Where things are

```
models/
  bronze/     staging views, one per source table (stg_*)
  silver/     shared joins and logic (int_*)
  gold/       marts + exposures + unit tests (mart_*)
  semantic/   semantic models, metrics, saved queries, time spine
seeds/        raw CSV extracts
macros/       shared SQL (cents_to_dollars, fiscal_quarter, …)
charts/       dbt Charts boards
docs/         these pages
```

## Next

- [Repo conventions](conventions.md): layers, naming and how data flows
- [Testing and data quality](testing-and-data-quality.md): what we test and how
- How-tos: [Working with DuckDB](how-to/duckdb.md), [Semantic layer and metrics](how-to/semantic-layer-and-metrics.md)
