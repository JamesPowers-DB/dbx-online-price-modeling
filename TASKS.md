# Build Checklist — Hardware Retailer Pricing Demo

Track build status for the demo implementation.

---

## Phase 1: Infrastructure

- [x] Verify `jsp-demo-trove` workspace profile authentication
- [x] Confirm catalog: `jsp_demo_trove_catalog` (no `retail_sku`, using existing catalog)
- [x] Create directory structure: `src/notebooks/`, `src/pipelines/`, `resources/`
- [x] Create `databricks.yml` with DABs bundle config

## Phase 2: DABs Resources

- [x] `resources/data_generator_job.yml` — one-shot data seeding job
- [x] `resources/streaming_ingest_job.yml` — multi-task bronze + silver job
- [x] `resources/gold_pipeline.yml` — SDP pipeline config

## Phase 3: Source Notebooks

- [x] `src/notebooks/00_data_generator.py`
  - [x] Schema + volume setup (raw, bronze, silver, gold, ml)
  - [x] `raw.product_catalog` (~500 SKUs, hardware categories)
  - [x] `raw.inventory_events` (~15K events)
  - [x] `raw.competitor_prices` (~25K scrape events)
  - [x] `raw.daily_sales` (~90K rows, 180-day history)
  - [x] `raw.customer_behavior` (~500 SKUs)
  - [x] `raw.promotions` (~2K rows)
  - [x] `raw.supplier_cost_changes` (~5K events)
  - [x] `raw.macro_signals` (~520 weekly PPI rows)

- [x] `src/notebooks/01_bronze_streaming_ingest.py`
  - [x] `readStream` from `raw.competitor_prices`
  - [x] `readStream` from `raw.inventory_events`
  - [x] Add metadata: `_ingested_at`, `_source`, `_job_run_id`
  - [x] `writeStream` to `bronze.competitor_prices_raw`
  - [x] `writeStream` to `bronze.inventory_events_raw`
  - [x] `trigger(availableNow=True)`
  - [x] Checkpoint volumes

- [x] `src/notebooks/02_silver_transform.py`
  - [x] Watermarking + dropDuplicates on `(sku_id, competitor, event_timestamp)`
  - [x] Schema validation (null checks, range checks)
  - [x] Enrich competitor prices with `product_catalog` (broadcast join)
  - [x] Inventory aggregation with `days_of_supply`
  - [x] Write to `silver.competitor_prices` and `silver.inventory`

## Phase 4: Spark Declarative Pipeline

- [x] `src/pipelines/gold_features/transformations/gold_pricing_features.sql`
  - [x] `CREATE OR REFRESH STREAMING TABLE` with `CLUSTER BY (sku_id)`
  - [x] Stream from `silver.competitor_prices`
  - [x] Join `raw.product_catalog` (static)
  - [x] Subquery: latest inventory per SKU from `silver.inventory`
  - [x] Subquery: 7d + 30d rolling demand from `raw.daily_sales`
  - [x] Left join `raw.customer_behavior`
  - [x] Left join `raw.promotions` (active only)
  - [x] Left join `raw.supplier_cost_changes` (90-day avg)
  - [x] Cross join macro signals (trailing 4-week avg PPI)

- [x] `src/pipelines/gold_features/transformations/gold_feature_mart.sql`
  - [x] `CREATE OR REFRESH MATERIALIZED VIEW`
  - [x] Dedup to 1 row per SKU (latest `_updated_at`)
  - [x] `CLUSTER BY (sku_id)`

## Phase 5: ML Notebooks

- [x] `src/notebooks/04_model_train_register.py`
  - [x] Read `gold.gold_feature_mart`
  - [x] Synthetic `optimal_price` label generation (realistic business logic)
  - [x] LightGBM training (19 features)
  - [x] `mlflow.lightgbm.autolog()`
  - [x] UC model registry: `jsp_demo_trove_catalog.ml.pricing_model`
  - [x] Champion alias
  - [x] Serverless Model Serving endpoint deployment via SDK

- [x] `src/notebooks/04_pricing_endpoint_demo.py`
  - [x] Load 10 sample SKUs from `gold_feature_mart`
  - [x] Query endpoint via Databricks SDK
  - [x] Comparison table: current vs. recommended price, delta%, signal
  - [x] Bar chart: current vs. recommended by SKU
  - [x] Horizontal bar chart: % opportunity by SKU
  - [x] Summary: GM% before/after

- [x] `src/notebooks/05_LIVE_DEMO_genie_code.py`
  - [x] Presenter instructions (markdown)
  - [x] `compute_elasticity_score()` stub with docstring
  - [x] `flag_sku_action()` stub with docstring
  - [x] Test cell
  - [x] Write results to `gold.pricing_recommendations`
  - [x] Reference implementation in collapsed cell (presenter backup)

## Phase 6: Documentation

- [x] `DEMO.md` — architecture, talking points, pre-demo checklist, contingency
- [x] `TASKS.md` — this file
- [x] `DEMO_SCRIPT.md` — 30-40 min timed presenter script
- [x] `README.md` — setup instructions, deploy commands

## Phase 7: Deployment & Verification

- [ ] `databricks bundle validate -t dev` — no schema errors
- [ ] `databricks bundle deploy -t dev` — resources created in workspace
- [ ] Run `data_generator_job` — all raw tables populated
- [ ] Run `streaming_ingest_job` — bronze + silver tables populated
- [ ] Start `gold_features_pipeline` — gold tables populated
- [ ] Run `04_model_train_register.py` — endpoint reaches READY
- [ ] Run `04_pricing_endpoint_demo.py` — output table and charts render cleanly
- [ ] Review `05_LIVE_DEMO_genie_code.py` — stubs are presenter-ready

---

## Notes

- Catalog: `jsp_demo_trove_catalog` (no `retail_sku` catalog exists in workspace)
- Serverless confirmed available in `jsp-demo-trove` workspace
- Checkpoint volumes: `/Volumes/{catalog}/{bronze_schema}/checkpoints`
- Model endpoint: `retail-pricing-endpoint` (serverless, scale-to-zero)
