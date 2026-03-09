# Online Hardware Retailer — Dynamic Pricing Demo

A production-realistic Databricks demo for a hardware retailer dynamic pricing system.
Showcases Unity Catalog, Delta streaming, Spark Declarative Pipelines, Serverless compute,
LightGBM + MLflow model training, and Serverless Model Serving.

**Catalog**: `jsp_demo_trove_catalog` | **Profile**: `jsp-demo-trove`

---

## Project Structure

```
dbx-online-price-modeling/
├── databricks.yml                    # Main DABs bundle config
├── resources/
│   ├── data_generator_job.yml        # Job: seed all raw source tables
│   ├── streaming_ingest_job.yml      # Job: bronze + silver streaming ingest
│   └── gold_pipeline.yml             # SDP: silver → gold feature pipeline
├── src/
│   ├── notebooks/
│   │   ├── 00_data_generator.py      # Seed 8 raw Delta tables (~135K rows)
│   │   ├── 01_bronze_streaming_ingest.py
│   │   ├── 02_silver_transform.py
│   │   ├── 04_model_train_register.py
│   │   ├── 04_pricing_endpoint_demo.py
│   │   └── 05_LIVE_DEMO_genie_code.py
│   └── pipelines/
│       └── gold_features/
│           └── transformations/
│               ├── gold_pricing_features.sql   # Streaming table (7-table join)
│               └── gold_feature_mart.sql       # Materialized view (1 row/SKU)
├── DEMO.md                           # Architecture + talking points + pre-demo checklist
├── TASKS.md                          # Build status checklist
└── DEMO_SCRIPT.md                    # 30-40 min presenter script
```

---

## Prerequisites

- Databricks CLI configured with `jsp-demo-trove` profile
- `databricks` CLI v0.200+

Verify auth:
```bash
databricks auth env --profile jsp-demo-trove
```

---

## Deploy

### 1. Validate Bundle

```bash
databricks bundle validate -t dev --profile jsp-demo-trove
```

### 2. Deploy Resources

```bash
databricks bundle deploy -t dev --profile jsp-demo-trove
```

This creates in the workspace:
- Job: `[dev] 00 - Seed Raw Source Tables`
- Job: `[dev] 01-02 - Bronze + Silver Streaming Ingest`
- Pipeline: `[dev] 03 - Gold Pricing Features Pipeline`

### 3. Seed Raw Data (run once)

```bash
databricks bundle run data_generator_job -t dev --profile jsp-demo-trove
```

Or run `00_data_generator.py` interactively in a notebook.

### 4. Run Streaming Ingest

```bash
databricks bundle run streaming_ingest_job -t dev --profile jsp-demo-trove
```

### 5. Run Gold SDP Pipeline

```bash
databricks bundle run gold_features_pipeline -t dev --profile jsp-demo-trove
```

### 6. Train Model + Deploy Endpoint

Run `04_model_train_register.py` interactively. Wait for endpoint `READY` (~5-10 min).

### 7. Test Endpoint Demo

Run `04_pricing_endpoint_demo.py` interactively.

---

## Demo Data Schema

### `raw.product_catalog`
| Column | Type | Description |
|--------|------|-------------|
| sku_id | STRING | Primary key (SKU-00001 ... SKU-00NNN) |
| product_name | STRING | Brand + type + variant |
| category | STRING | Tools / Fasteners / Electrical / Plumbing / Paint / Safety |
| brand | STRING | DeWalt, Milwaukee, SharkBite, etc. |
| unit_cost | DOUBLE | Wholesale cost in USD |

### `gold.gold_feature_mart`
One row per SKU (latest pricing snapshot). 23 features + metadata.

Key columns: `sku_id`, `current_price`, `competitor_price`, `current_stock`,
`days_of_supply`, `units_sold_7d`, `units_sold_30d`, `cart_add_rate`,
`conversion_rate`, `discount_pct`, `ppi_steel`, `ppi_copper`, `ppi_pvc`

---

## Catalog Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `catalog` | `jsp_demo_trove_catalog` | Unity Catalog name |
| `raw_schema` | `raw` | Raw source Delta tables |
| `bronze_schema` | `bronze` | Bronze streaming tables |
| `silver_schema` | `silver` | Silver validated tables |
| `gold_schema` | `gold` | Gold feature tables |
| `ml_schema` | `ml` | ML models + experiments |

---

## Key Design Decisions

| Decision | Choice | Reason |
|----------|--------|--------|
| SDP language | SQL | Cleaner for demo, easier to narrate |
| ML model | LightGBM | Fast training, no heavy setup |
| Streaming source | Delta table as message bus | No Kafka needed for demo |
| Streaming trigger | `availableNow=True` | Clean bounded run for demo |
| MLflow | `autolog` only | Avoid rabbit holes |
| Compute | Serverless everywhere | Core demo message |
| Checkpoint location | UC Volume `/Volumes/{cat}/{schema}/checkpoints` | UC-native, no DBFS root |

---

## Cleanup

```bash
databricks bundle destroy -t dev --profile jsp-demo-trove
```

Destroys jobs and pipeline. Drop schemas manually:
```sql
DROP SCHEMA IF EXISTS jsp_demo_trove_catalog.raw CASCADE;
DROP SCHEMA IF EXISTS jsp_demo_trove_catalog.bronze CASCADE;
DROP SCHEMA IF EXISTS jsp_demo_trove_catalog.silver CASCADE;
DROP SCHEMA IF EXISTS jsp_demo_trove_catalog.gold CASCADE;
DROP SCHEMA IF EXISTS jsp_demo_trove_catalog.ml CASCADE;
```
