# Testing and data quality

What we check, where it's declared, and what to add when you write a new model. Every check runs as part of `dbt build`, so a failure stops the models downstream of it.

## The checks we use

| Check | Catches | Declared in | Example here |
|---|---|---|---|
| Generic data tests | Bad values in the data: nulls, duplicates, unexpected categories, broken keys | `data_tests:` on columns in the layer's `_*__models.yml` / `_*__sources.yml` | `unique` + `not_null` on every `*_id` primary key |
| Model contracts | A model's shape changing: a column dropped, renamed or retyped | `config: contract: enforced: true` + `data_type` on every column | [stg_customers](ref:stg_customers) |
| Unit tests | Logic bugs, on hand-written input rows, before any data is read | `_*__unit_tests.yml` | net amount + margin in [int_sales_enriched](ref:int_sales_enriched); segments in [mart_customer_360](ref:mart_customer_360) |
| Source freshness | Raw data that stopped arriving | `freshness:` + `loaded_at_field` in `_bronze__sources.yml` | [raw_sales](source:raw.raw_sales) warns after 1 day, errors after 3 |

## What every model needs

- [ ] A `description`, and descriptions for the columns people will query
- [ ] `unique` and `not_null` on the primary key (or a `dbt_utils.unique_combination_of_columns` test when the grain is several columns)
- [ ] `not_null` on foreign keys that should always be set, and a `relationships` test where a broken link would silently drop rows
- [ ] `accepted_values` on low-cardinality categories (channel, tier, device)
- [ ] A unit test when the model has branching logic (`case when`, incremental filters, window functions)

Bronze models that other teams depend on also get an enforced contract.

## Writing tests

### Generic tests

dbt 1.10 puts test arguments under `arguments:`:

```yaml
columns:
  - name: channel
    data_tests:
      - not_null
      - accepted_values:
          arguments:
            values: [web, mobile, store]
```

Use `config: severity: warn` for checks that should flag but not block, for example a soft range check on a price.

### Unit tests

A unit test gives a model fixed input rows and the rows it should produce:

```yaml
unit_tests:
  - name: test_int_sales_enriched_net_and_margin
    model: int_sales_enriched
    overrides:
      macros:
        is_incremental: false   # test the full-refresh path
    given:
      - input: ref('stg_sales')
        rows:
          - {order_id: ORD-1, customer_id: CUST-1, product_id: SKU-1, order_ts: "2025-03-10 10:00:00",
             quantity: 3, unit_price_dollars: 25.00, discount_dollars: 5.00, gross_amount_dollars: 75.00}
      - input: ref('stg_products')
        rows:
          - {product_id: SKU-1, cost_dollars: 10.00}
      - input: ref('stg_customers')
        rows:
          - {customer_id: CUST-1}
    expect:
      rows:
        - {order_id: ORD-1, net_amount_dollars: 70.00, gross_margin_dollars: 40.00}
```

Every `ref()` the model reads needs an entry under `given`, even with a single row. Within each, only list the columns the test is about; dbt fills the rest with nulls. The real test in `models/silver/_silver__unit_tests.yml` is the full version. Run unit tests alone with `dbt test --select test_type:unit`.

### Contracts

With `contract: enforced: true`, dbt checks the built columns and types against the YAML before it creates the model and fails the build on any difference. Changing a contracted model's columns is a breaking change: tell the people downstream, or version the model.

## Running checks

| Command | Runs |
|---|---|
| `dbt build` | Everything, in DAG order |
| `dbt test --select stg_sales` | Tests on one model |
| `dbt test --select test_type:unit` | Unit tests only |
| `dbt test --select source:raw` | Tests on the raw sources |
| `dbt source freshness` | Freshness checks |

When a test fails, `dbt build` prints its compiled SQL under `target/compiled/`. Running that query shows the failing rows. In wickd, a failed test's rows are one click away in the side panel.

## Known exceptions

- `raw_ga_sessions` and `raw_inventory_snapshots` have very loose freshness windows (800+ days) because the seeds are historical extracts. Tighten them once real loads arrive.
