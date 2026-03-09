# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 02 · Silver Transform
# MAGIC
# MAGIC **Layer**: Bronze → Silver
# MAGIC **Pattern**: Streaming dedup + validation + catalog enrichment
# MAGIC **Trigger**: `availableNow=True`
# MAGIC
# MAGIC ### What this notebook does
# MAGIC 1. Reads `bronze.competitor_prices_raw` and `bronze.inventory_events_raw` as streams
# MAGIC 2. **Deduplicates** using watermarking on `(sku_id, event_timestamp)`
# MAGIC 3. **Validates** schema (null checks, range constraints)
# MAGIC 4. **Enriches** competitor prices with `product_catalog` (join on `sku_id`)
# MAGIC 5. **Aggregates** inventory events → latest stock level per SKU
# MAGIC 6. Writes to `silver.competitor_prices` and `silver.inventory`
# MAGIC
# MAGIC ### Key concepts to highlight in demo
# MAGIC - **Watermarking** — stateful dedup with bounded state; handles late-arriving data
# MAGIC - **Schema validation** — data quality enforcement before data reaches analysts
# MAGIC - **Delta ACID** — all writes are transactional; no partial results visible to readers

# COMMAND ----------

dbutils.widgets.text("catalog", "jsp_demo_trove_catalog", "Catalog")
dbutils.widgets.text("raw_schema", "raw", "Raw Schema")
dbutils.widgets.text("bronze_schema", "bronze", "Bronze Schema")
dbutils.widgets.text("silver_schema", "silver", "Silver Schema")

catalog       = dbutils.widgets.get("catalog")
raw_schema    = dbutils.widgets.get("raw_schema")
bronze_schema = dbutils.widgets.get("bronze_schema")
silver_schema = dbutils.widgets.get("silver_schema")

checkpoint_base = f"/Volumes/{catalog}/{silver_schema}/checkpoints"

print(f"Source:      {catalog}.{bronze_schema}.*")
print(f"Target:      {catalog}.{silver_schema}.*")
print(f"Checkpoints: {checkpoint_base}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform: `silver.competitor_prices`
# MAGIC
# MAGIC Dedup + validate + enrich with product catalog

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

# --- Read bronze stream ---
df_bronze_comp = (
    spark.readStream
    .format("delta")
    .table(f"{catalog}.{bronze_schema}.competitor_prices_raw")
)

# --- Watermark for late data tolerance (30-minute window) ---
df_watermarked = df_bronze_comp.withWatermark("event_timestamp", "30 minutes")

# --- Deduplication: keep one event per (sku_id, competitor, event_timestamp) ---
df_deduped = df_watermarked.dropDuplicates(["sku_id", "competitor", "event_timestamp"])

# --- Schema validation: drop records with null keys or invalid prices ---
df_validated = (
    df_deduped
    .filter(F.col("sku_id").isNotNull())
    .filter(F.col("competitor_price") > 0)
    .filter(F.col("competitor_price") < 10_000)  # sanity cap
    .filter(F.col("event_timestamp").isNotNull())
)

# --- Enrich: join with product catalog (static lookup) ---
df_catalog = spark.table(f"{catalog}.{raw_schema}.product_catalog").select(
    "sku_id", "category", "subcategory", "brand", "unit_cost", "product_name"
)

df_enriched = df_validated.join(F.broadcast(df_catalog), on="sku_id", how="inner")

# --- Add silver metadata ---
df_silver_comp = df_enriched.withColumns({
    "_validated_at":    F.current_timestamp(),
    "_price_to_cost":   F.round(F.col("competitor_price") / F.col("unit_cost"), 4),
})

# --- Write to silver ---
(
    df_silver_comp.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{checkpoint_base}/competitor_prices")
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(f"{catalog}.{silver_schema}.competitor_prices")
    .awaitTermination()
)

print(f"✓ Wrote to {catalog}.{silver_schema}.competitor_prices")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform: `silver.inventory`
# MAGIC
# MAGIC Dedup + aggregate to latest stock level per SKU

# COMMAND ----------

# --- Read bronze inventory stream ---
df_bronze_inv = (
    spark.readStream
    .format("delta")
    .table(f"{catalog}.{bronze_schema}.inventory_events_raw")
)

# --- Watermark + dedup ---
df_inv_deduped = (
    df_bronze_inv
    .withWatermark("event_timestamp", "30 minutes")
    .dropDuplicates(["sku_id", "event_timestamp"])
)

# --- Validate ---
df_inv_validated = (
    df_inv_deduped
    .filter(F.col("sku_id").isNotNull())
    .filter(F.col("stock_after") >= 0)
)

# --- Compute days_of_supply using 30-day avg daily sales (from raw) ---
df_avg_sales = (
    spark.table(f"{catalog}.{raw_schema}.daily_sales")
    .filter(F.col("sale_date") >= F.date_sub(F.current_date(), 30))
    .groupBy("sku_id")
    .agg(F.avg("units_sold").alias("avg_daily_units"))
)

# Append-mode streaming: keep event-level granularity, enrich with sales velocity
df_inv_silver = (
    df_inv_validated
    .join(F.broadcast(df_avg_sales), on="sku_id", how="left")
    .withColumn(
        "days_of_supply",
        F.when(
            F.col("avg_daily_units") > 0,
            F.round(F.col("stock_after") / F.col("avg_daily_units"), 1)
        ).otherwise(F.lit(None).cast("double"))
    )
    .withColumn("current_stock", F.col("stock_after"))
    .withColumn("_validated_at", F.current_timestamp())
)

(
    df_inv_silver.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{checkpoint_base}/inventory")
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(f"{catalog}.{silver_schema}.inventory")
    .awaitTermination()
)

print(f"✓ Wrote to {catalog}.{silver_schema}.inventory")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Silver Tables

# COMMAND ----------

for table, key_col in [("competitor_prices", "sku_id"), ("inventory", "sku_id")]:
    df = spark.table(f"{catalog}.{silver_schema}.{table}")
    count = df.count()
    distinct_skus = df.select(key_col).distinct().count()
    latest = df.selectExpr("max(_validated_at) as latest").first()["latest"]

    print(f"  {catalog}.{silver_schema}.{table}")
    print(f"    Rows: {count:,}  |  Distinct SKUs: {distinct_skus:,}  |  Latest: {latest}")
    print()
