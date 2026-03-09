# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 04 · Pricing Endpoint — Live Demo
# MAGIC
# MAGIC **Purpose**: Customer-facing demo of the real-time pricing recommendation system.
# MAGIC
# MAGIC This notebook shows the end-to-end value story:
# MAGIC > *"For any SKU, we can now ask: is our current shelf price leaving money on the table, or are we pricing ourselves out of the market?"*
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### What we'll do
# MAGIC 1. Pull a sample of 10 SKUs from the gold feature mart
# MAGIC 2. Call the **serverless model serving endpoint** in real time
# MAGIC 3. Compare current price vs. recommended price
# MAGIC 4. Visualize the pricing opportunity

# COMMAND ----------

dbutils.widgets.text("catalog",       "hardware_sku_demo",   "Catalog")
dbutils.widgets.text("gold_schema",   "gold",                     "Gold Schema")
dbutils.widgets.text("endpoint_name", "retail-pricing-endpoint",  "Endpoint Name")

catalog       = dbutils.widgets.get("catalog")
gold_schema   = dbutils.widgets.get("gold_schema")
endpoint_name = dbutils.widgets.get("endpoint_name")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 · Sample 10 Representative SKUs
# MAGIC
# MAGIC Selecting a diverse cross-section: tools, fasteners, electrical, plumbing

# COMMAND ----------

import pandas as pd
from pyspark.sql import functions as F

FEATURE_COLS = [
    "sku_id", "product_name", "category", "brand",
    "unit_cost", "current_price",
    "competitor_price", "competitor_markup",
    "current_stock", "days_of_supply",
    "units_sold_7d", "units_sold_30d", "avg_daily_units", "seasonality_index",
    "view_count", "cart_add_rate", "conversion_rate", "return_rate",
    "discount_pct",
    "cost_delta_pct",
    "ppi_steel", "ppi_copper", "ppi_pvc", "weight_lbs"
]

MODEL_FEATURE_COLS = [c for c in FEATURE_COLS if c not in (
    "sku_id", "product_name", "category", "brand", "current_price"
)]

# Pull diverse sample across categories
df_sample = (
    spark.table(f"{catalog}.{gold_schema}.gold_feature_mart")
    .select(*FEATURE_COLS)
    .orderBy(F.rand(seed=99))
    .limit(10)
)

pdf_sample = df_sample.toPandas()

display(
    df_sample.select("sku_id", "product_name", "category", "brand",
                     "unit_cost", "current_price", "competitor_price",
                     "units_sold_7d", "days_of_supply")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 · Call Pricing Endpoint in Real Time
# MAGIC
# MAGIC The endpoint runs on **Serverless Model Serving** — scales to zero, instant cold start

# COMMAND ----------

from databricks.sdk import WorkspaceClient
import json

w = WorkspaceClient()

# Build inference request payload
feature_rows = pdf_sample[MODEL_FEATURE_COLS].fillna(0).to_dict(orient="records")
payload = {"dataframe_records": feature_rows}

# Call the endpoint
response = w.serving_endpoints.query(
    name=endpoint_name,
    dataframe_records=feature_rows,
)

predictions = response.predictions
print(f"Received {len(predictions)} price recommendations from endpoint '{endpoint_name}'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 · Build Pricing Comparison Table

# COMMAND ----------

pdf_results = pdf_sample[["sku_id", "product_name", "category", "brand",
                            "unit_cost", "current_price", "competitor_price",
                            "units_sold_7d"]].copy()

pdf_results["recommended_price"] = [round(p, 2) for p in predictions]
pdf_results["delta_pct"] = (
    (pdf_results["recommended_price"] - pdf_results["current_price"])
    / pdf_results["current_price"] * 100
).round(1)
pdf_results["signal"] = pdf_results["delta_pct"].apply(
    lambda d: "↑ RAISE" if d > 3 else ("↓ DISCOUNT" if d < -3 else "→ HOLD")
)
pdf_results["current_gm_pct"] = (
    (pdf_results["current_price"] - pdf_results["unit_cost"])
    / pdf_results["current_price"] * 100
).round(1)
pdf_results["recommended_gm_pct"] = (
    (pdf_results["recommended_price"] - pdf_results["unit_cost"])
    / pdf_results["recommended_price"] * 100
).round(1)

display_cols = [
    "sku_id", "product_name", "category",
    "unit_cost", "current_price", "competitor_price",
    "recommended_price", "delta_pct", "signal",
    "current_gm_pct", "recommended_gm_pct",
]

print("\n=== PRICING RECOMMENDATIONS ===\n")
print(pdf_results[display_cols].to_string(index=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 · Visualize: Current vs. Recommended Price

# COMMAND ----------

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np

fig, axes = plt.subplots(1, 2, figsize=(16, 6))
fig.suptitle("AI-Powered Pricing Recommendations — Hardware Retailer Demo", fontsize=14, fontweight="bold")

# --- Left: Price Comparison Bar Chart ---
ax = axes[0]
x = np.arange(len(pdf_results))
width = 0.35

bars_current = ax.bar(x - width/2, pdf_results["current_price"],   width, label="Current Price",     color="#1f77b4", alpha=0.85)
bars_rec     = ax.bar(x + width/2, pdf_results["recommended_price"], width, label="Recommended Price", color="#2ca02c", alpha=0.85)

# Color recommended bars by signal
for i, (bar, signal) in enumerate(zip(bars_rec, pdf_results["signal"])):
    if "RAISE" in signal:
        bar.set_color("#d62728")   # red = raise
    elif "DISCOUNT" in signal:
        bar.set_color("#ff7f0e")   # orange = discount
    else:
        bar.set_color("#2ca02c")   # green = hold

ax.set_xlabel("SKU")
ax.set_ylabel("Price ($)")
ax.set_title("Current vs. Recommended Price by SKU")
ax.set_xticks(x)
ax.set_xticklabels([s[:8] for s in pdf_results["sku_id"]], rotation=45, ha="right", fontsize=8)

patches = [
    mpatches.Patch(color="#1f77b4", label="Current Price"),
    mpatches.Patch(color="#d62728", label="Recommend Raise"),
    mpatches.Patch(color="#ff7f0e", label="Recommend Discount"),
    mpatches.Patch(color="#2ca02c", label="Hold"),
]
ax.legend(handles=patches, fontsize=8)
ax.grid(axis="y", alpha=0.3)

# --- Right: % Delta Distribution ---
ax2 = axes[1]
colors = ["#d62728" if d > 3 else "#ff7f0e" if d < -3 else "#2ca02c"
          for d in pdf_results["delta_pct"]]
bars = ax2.barh(
    [f"{r['sku_id']}\n{r['product_name'][:20]}" for _, r in pdf_results.iterrows()],
    pdf_results["delta_pct"],
    color=colors,
    alpha=0.85,
)
ax2.axvline(0, color="black", linewidth=0.8)
ax2.set_xlabel("Price Change Recommendation (%)")
ax2.set_title("Pricing Opportunity by SKU")
ax2.grid(axis="x", alpha=0.3)

# Add value labels
for bar, val in zip(bars, pdf_results["delta_pct"]):
    x_pos = val + (0.2 if val >= 0 else -0.2)
    ax2.text(x_pos, bar.get_y() + bar.get_height() / 2,
             f"{val:+.1f}%", va="center", ha="left" if val >= 0 else "right",
             fontsize=8, fontweight="bold")

plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary: Pricing Opportunity

# COMMAND ----------

total_skus   = len(pdf_results)
raise_count  = (pdf_results["delta_pct"] > 3).sum()
hold_count   = (pdf_results["delta_pct"].abs() <= 3).sum()
disc_count   = (pdf_results["delta_pct"] < -3).sum()

avg_delta_raise = pdf_results.loc[pdf_results["delta_pct"] > 3, "delta_pct"].mean()
avg_delta_disc  = pdf_results.loc[pdf_results["delta_pct"] < -3, "delta_pct"].mean()

print("=" * 55)
print("  PRICING RECOMMENDATION SUMMARY")
print("=" * 55)
print(f"  Total SKUs analyzed:    {total_skus}")
print(f"  Recommend RAISE:        {raise_count}  (avg +{avg_delta_raise:.1f}%)")
print(f"  Recommend HOLD:         {hold_count}")
print(f"  Recommend DISCOUNT:     {disc_count}  (avg {avg_delta_disc:.1f}%)")
print("=" * 55)
print(f"\n  Avg GM% today:          {pdf_results['current_gm_pct'].mean():.1f}%")
print(f"  Avg GM% recommended:    {pdf_results['recommended_gm_pct'].mean():.1f}%")
print(f"  Estimated GM improvement: +{(pdf_results['recommended_gm_pct'].mean() - pdf_results['current_gm_pct'].mean()):.1f} pts")
print()
print("Next step: Open '05_LIVE_DEMO_genie_code' to implement price elasticity flagging live")
