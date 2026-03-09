# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 05 · Live Coding: Price Elasticity Flagging
# MAGIC
# MAGIC ## Presenter Instructions
# MAGIC
# MAGIC > **This is the live coding segment.** Use Databricks Assistant to implement the
# MAGIC > `flag_sku_action` function below. Narrate what you're asking the assistant to do.
# MAGIC >
# MAGIC > **Suggested prompt for Databricks Assistant:**
# MAGIC > *"Write a Python function that classifies a SKU as RAISE, HOLD, or DISCOUNT based
# MAGIC > on elasticity score, margin gap to target, and competitive position. Include docstring."*
# MAGIC >
# MAGIC > **Time allocation:** ~5 minutes
# MAGIC > - 1 min: Explain the business problem (elasticity flagging)
# MAGIC > - 2 min: Type the assistant prompt, show autocomplete in action
# MAGIC > - 1 min: Run the cell, show results table
# MAGIC > - 1 min: Write results back to Delta, show Unity Catalog lineage

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

dbutils.widgets.text("catalog",     "jsp_demo_trove_catalog", "Catalog")
dbutils.widgets.text("gold_schema", "gold",                   "Gold Schema")

catalog     = dbutils.widgets.get("catalog")
gold_schema = dbutils.widgets.get("gold_schema")

import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Load feature mart for all SKUs
df_mart = spark.table(f"{catalog}.{gold_schema}.gold_feature_mart")
pdf_mart = df_mart.toPandas()

print(f"Loaded {len(pdf_mart):,} SKUs from gold_feature_mart")
print(f"Columns: {list(pdf_mart.columns)[:8]} ...")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Live Coding: Implement `flag_sku_action`
# MAGIC
# MAGIC **👇 Use Databricks Assistant here — invoke with Cmd+I (Mac) or Ctrl+I (Windows)**
# MAGIC
# MAGIC Ask it to write the function body. The docstring and signature are pre-written.

# COMMAND ----------

# Configuration: thresholds for price action signals
TARGET_GM_PCT   = 0.42   # Target gross margin (42%)
RAISE_THRESHOLD = 0.03   # Recommend RAISE if recommended > current by 3%+
DISC_THRESHOLD  = 0.03   # Recommend DISCOUNT if recommended < current by 3%+
ELASTICITY_CAP  = 2.0    # Price elasticity estimate (simplified)

def compute_elasticity_score(row: pd.Series) -> float:
    """
    Compute a simplified price elasticity score for a SKU.

    Price elasticity = % change in demand / % change in price.
    Here we proxy demand sensitivity using cart_add_rate and conversion_rate.
    A higher score means demand is MORE sensitive to price changes.

    Args:
        row: A pandas Series representing one SKU row from gold_feature_mart.

    Returns:
        float: Elasticity score (0.0 = inelastic, 1.0+ = highly elastic).
    """
    # TODO: Implement with Databricks Assistant
    # Hint: use row["conversion_rate"], row["cart_add_rate"], row["avg_daily_units"]
    # Higher conversion + high velocity = inelastic (willing to pay)
    # Low conversion + low velocity = elastic (price-sensitive)
    pass


def flag_sku_action(
    row: pd.Series,
    target_gm:       float = TARGET_GM_PCT,
    raise_threshold: float = RAISE_THRESHOLD,
    disc_threshold:  float = DISC_THRESHOLD,
) -> dict:
    """
    Classify a SKU as RAISE, HOLD, or DISCOUNT based on pricing signals.

    Decision logic:
    - RAISE:    Margin below target AND elasticity is low AND not undercut by competitors
    - DISCOUNT: Competitor significantly cheaper AND conversion dropping
    - HOLD:     Otherwise — price is reasonable, monitor

    Args:
        row:             pandas Series with SKU features from gold_feature_mart.
        target_gm:       Target gross margin percentage (e.g. 0.42 for 42%).
        raise_threshold: Min price delta % to trigger RAISE recommendation.
        disc_threshold:  Min price delta % to trigger DISCOUNT recommendation.

    Returns:
        dict with keys: sku_id, action, elasticity_score, margin_gap_pct, rationale
    """
    # TODO: Implement with Databricks Assistant
    pass


# COMMAND ----------

# MAGIC %md
# MAGIC ## Test Cell — Run after implementing above

# COMMAND ----------

# Apply flag function to all SKUs
results = pdf_mart.apply(flag_sku_action, axis=1)
pdf_flagged = pd.DataFrame(list(results))

# Display results
print("\n=== PRICE ACTION FLAGS ===\n")
print(pdf_flagged.groupby("action").size().rename("count").to_string())
print()

display(
    spark.createDataFrame(pdf_flagged)
    .orderBy("action", "margin_gap_pct")
    .limit(20)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Results to `gold.pricing_recommendations`
# MAGIC
# MAGIC Demonstrating the full loop: AI recommendation → Delta table → Unity Catalog lineage

# COMMAND ----------

from pyspark.sql import functions as F

df_recs = (
    spark.createDataFrame(pdf_flagged)
    .withColumn("generated_at", F.current_timestamp())
    .withColumn("model_version", F.lit("live_demo_v1"))
)

(
    df_recs
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.{gold_schema}.pricing_recommendations")
)

count = spark.table(f"{catalog}.{gold_schema}.pricing_recommendations").count()
print(f"✓ Wrote {count:,} pricing recommendations to {catalog}.{gold_schema}.pricing_recommendations")
print("\n  → Open Unity Catalog to see data lineage from gold_feature_mart → pricing_recommendations")
print("  → This table is now available to the BI team in SQL Editor or a dashboard")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reference: Full Implementation (Presenter Backup)
# MAGIC
# MAGIC *Expand only if live coding gets stuck*

# COMMAND ----------

# PRESENTER BACKUP — collapse this cell before the demo

def compute_elasticity_score_ref(row: pd.Series) -> float:
    """Reference implementation of elasticity score."""
    # Proxy: high conversion rate + high velocity = inelastic (willing to pay)
    # Low conversion + low days_of_supply = elastic (hold on deals)
    conversion = row.get("conversion_rate", 0.1)
    velocity   = min(row.get("avg_daily_units", 1) / 10, 1.0)  # normalize to 0-1
    inelastic  = (conversion * 0.6) + (velocity * 0.4)         # 0=inelastic, 1=elastic
    elasticity = 1.0 - inelastic                                 # invert: high = elastic
    return round(max(0.0, min(1.0, elasticity)), 4)


def flag_sku_action_ref(row: pd.Series, target_gm=0.42, raise_threshold=0.03, disc_threshold=0.03) -> dict:
    """Reference implementation of flag_sku_action."""
    current_price = row.get("current_price", row["unit_cost"] * 1.35)
    unit_cost     = row["unit_cost"]
    comp_price    = row["competitor_price"]
    current_gm    = (current_price - unit_cost) / current_price if current_price > 0 else 0
    margin_gap    = round((current_gm - target_gm) * 100, 2)  # + means above target
    elasticity    = compute_elasticity_score_ref(row)
    comp_gap_pct  = (current_price - comp_price) / comp_price if comp_price > 0 else 0

    # Decision tree
    if margin_gap < -5 and elasticity < 0.5 and comp_gap_pct < 0.05:
        action    = "RAISE"
        rationale = f"Margin {margin_gap:+.1f}pts below target; low elasticity ({elasticity:.2f}); competitive"
    elif comp_gap_pct > raise_threshold and row.get("conversion_rate", 0.1) < 0.08:
        action    = "DISCOUNT"
        rationale = f"Priced {comp_gap_pct*100:.1f}% above competitor; low conversion ({row.get('conversion_rate',0):.3f})"
    else:
        action    = "HOLD"
        rationale = f"Margin gap {margin_gap:+.1f}pts; elasticity {elasticity:.2f}; monitor"

    return {
        "sku_id":           row["sku_id"],
        "action":           action,
        "elasticity_score": elasticity,
        "margin_gap_pct":   margin_gap,
        "current_gm_pct":   round(current_gm * 100, 2),
        "comp_gap_pct":     round(comp_gap_pct * 100, 2),
        "rationale":        rationale,
    }
