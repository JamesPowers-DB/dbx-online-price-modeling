# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 01 · Bronze Streaming Ingest
# MAGIC
# MAGIC **Layer**: Raw → Bronze
# MAGIC **Pattern**: `spark.readStream` from Delta source tables (Delta-as-message-bus)
# MAGIC **Trigger**: `availableNow=True` — processes all new data, then stops cleanly
# MAGIC
# MAGIC ### What this notebook does
# MAGIC 1. Reads `raw.competitor_prices` and `raw.inventory_events` as Delta streams
# MAGIC 2. Adds metadata columns: `_ingested_at`, `_source`, `_job_run_id`
# MAGIC 3. Writes to `bronze.competitor_prices_raw` and `bronze.inventory_events_raw`
# MAGIC
# MAGIC ### Key concepts to highlight in demo
# MAGIC - **Delta as a message bus** — no Kafka needed; Delta change log acts as the stream source
# MAGIC - **Checkpointing** — exactly-once semantics; stream restarts from where it left off
# MAGIC - **`availableNow=True`** — modern trigger that processes a bounded batch, perfect for scheduled jobs

# COMMAND ----------

dbutils.widgets.text("catalog", "jsp_demo_trove_catalog", "Catalog")
dbutils.widgets.text("raw_schema", "raw", "Raw Schema")
dbutils.widgets.text("bronze_schema", "bronze", "Bronze Schema")

catalog       = dbutils.widgets.get("catalog")
raw_schema    = dbutils.widgets.get("raw_schema")
bronze_schema = dbutils.widgets.get("bronze_schema")

checkpoint_base = f"/Volumes/{catalog}/{bronze_schema}/checkpoints"

print(f"Source:      {catalog}.{raw_schema}.*")
print(f"Target:      {catalog}.{bronze_schema}.*")
print(f"Checkpoints: {checkpoint_base}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest: `competitor_prices_raw`

# COMMAND ----------

from pyspark.sql import functions as F

# Read from raw Delta table as a stream
# Delta acts as an append-only message bus — each new row is a streaming event
df_comp_stream = (
    spark.readStream
    .format("delta")
    .option("ignoreDeletes", "true")
    .table(f"{catalog}.{raw_schema}.competitor_prices")
)

# Add bronze metadata columns
df_comp_bronze = df_comp_stream.withColumns({
    "_ingested_at": F.current_timestamp(),
    "_source":      F.lit(f"{catalog}.{raw_schema}.competitor_prices"),
})

# Write to bronze with checkpointing for exactly-once semantics
(
    df_comp_bronze.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{checkpoint_base}/competitor_prices_raw")
    .option("mergeSchema", "true")
    .trigger(availableNow=True)          # Process all pending data, then stop
    .toTable(f"{catalog}.{bronze_schema}.competitor_prices_raw")
    .awaitTermination()
)

print(f"✓ Wrote to {catalog}.{bronze_schema}.competitor_prices_raw")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest: `inventory_events_raw`

# COMMAND ----------

df_inv_stream = (
    spark.readStream
    .format("delta")
    .option("ignoreDeletes", "true")
    .table(f"{catalog}.{raw_schema}.inventory_events")
)

df_inv_bronze = df_inv_stream.withColumns({
    "_ingested_at": F.current_timestamp(),
    "_source":      F.lit(f"{catalog}.{raw_schema}.inventory_events"),
})

(
    df_inv_bronze.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{checkpoint_base}/inventory_events_raw")
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(f"{catalog}.{bronze_schema}.inventory_events_raw")
    .awaitTermination()
)

print(f"✓ Wrote to {catalog}.{bronze_schema}.inventory_events_raw")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Bronze Tables

# COMMAND ----------

for table in ["competitor_prices_raw", "inventory_events_raw"]:
    df = spark.table(f"{catalog}.{bronze_schema}.{table}")
    count = df.count()
    latest = df.selectExpr("max(_ingested_at) as latest").first()["latest"]
    print(f"  {catalog}.{bronze_schema}.{table}")
    print(f"    Rows: {count:,}  |  Latest ingest: {latest}")
    print()
