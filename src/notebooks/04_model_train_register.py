# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 04 · Model Training, Registration & Deployment
# MAGIC
# MAGIC **Layer**: Gold Feature Mart → ML Model → Serving Endpoint
# MAGIC
# MAGIC ### What this notebook does
# MAGIC 1. Reads `gold.gold_feature_mart` — the ML-ready feature table
# MAGIC 2. Generates a synthetic `optimal_price` label (cost × markup + demand signal + noise)
# MAGIC 3. Trains a **LightGBM** price recommendation model
# MAGIC 4. Logs everything to **MLflow** (autolog: params, metrics, feature importance)
# MAGIC 5. Registers the model to **Unity Catalog** (`ml.pricing_model`)
# MAGIC 6. Deploys a **Serverless Model Serving** endpoint
# MAGIC
# MAGIC ### Key concepts to highlight in demo
# MAGIC - **MLflow autolog** — one line captures params, metrics, model artifact automatically
# MAGIC - **Unity Catalog Model Registry** — governed, versioned, lineage-tracked models
# MAGIC - **Serverless Model Serving** — zero cluster management; scales to zero when idle

# COMMAND ----------

# MAGIC %pip install lightgbm mlflow --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog",    "hardware_sku_demo", "Catalog")
dbutils.widgets.text("gold_schema","gold",                   "Gold Schema")
dbutils.widgets.text("ml_schema",  "ml",                    "ML Schema")
dbutils.widgets.text("endpoint_name", "retail-pricing-endpoint", "Endpoint Name")

catalog        = dbutils.widgets.get("catalog")
gold_schema    = dbutils.widgets.get("gold_schema")
ml_schema      = dbutils.widgets.get("ml_schema")
endpoint_name  = dbutils.widgets.get("endpoint_name")
model_name     = f"{catalog}.{ml_schema}.pricing_model"

print(f"Feature table:  {catalog}.{gold_schema}.gold_feature_mart")
print(f"Model registry: {model_name}")
print(f"Endpoint:       {endpoint_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 · Load Features from Gold Mart

# COMMAND ----------

import mlflow
import numpy as np
import pandas as pd
import lightgbm as lgb

from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, r2_score

# Read the ML-ready feature mart
df_features = spark.table(f"{catalog}.{gold_schema}.gold_feature_mart")
print(f"Feature mart: {df_features.count():,} SKUs")
display(df_features.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 · Generate Synthetic `optimal_price` Target
# MAGIC
# MAGIC In a production system this would be:
# MAGIC - Historical price-response modeling (A/B test outcomes)
# MAGIC - Constrained optimization (margin floor + competitive ceiling)
# MAGIC
# MAGIC For the demo we simulate a "ground truth" price that accounts for:
# MAGIC - Cost markup (40-60% GM target)
# MAGIC - Competitive pressure (pull toward lower competitor if within 10%)
# MAGIC - Demand signal (high velocity → price up; low stock → price up)
# MAGIC - Active promo (lock to discounted price)

# COMMAND ----------

pdf = df_features.toPandas()

np.random.seed(42)

def compute_optimal_price(row):
    """Simulate ground-truth optimal price for training labels."""
    target_gm      = np.random.uniform(0.38, 0.55)
    cost_floor     = row["unit_cost"] / (1 - target_gm)

    # Competitive adjustment: nudge toward competitor if within 10%
    comp_delta = row["competitor_price"] - cost_floor
    comp_adj   = 0.4 * comp_delta if abs(comp_delta / cost_floor) < 0.10 else 0.0

    # Demand signal: high demand → charge more
    demand_adj = 0.0
    if row["avg_daily_units"] > 5:
        demand_adj = cost_floor * 0.03
    elif row["avg_daily_units"] < 1:
        demand_adj = -cost_floor * 0.04

    # Scarcity signal: low days of supply → charge more
    scarcity_adj = cost_floor * 0.02 if 0 < row["days_of_supply"] < 14 else 0.0

    # Promo override
    if row["discount_pct"] > 0:
        return round(cost_floor * (1 - row["discount_pct"]) + np.random.normal(0, 0.5), 2)

    price = cost_floor + comp_adj + demand_adj + scarcity_adj
    price += np.random.normal(0, cost_floor * 0.01)  # label noise

    return round(max(row["unit_cost"] * 1.10, price), 2)

pdf["optimal_price"] = pdf.apply(compute_optimal_price, axis=1)
print(f"Label stats:\n{pdf['optimal_price'].describe().round(2)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 · Train LightGBM + Log with MLflow

# COMMAND ----------

FEATURE_COLS = [
    "unit_cost", "weight_lbs",
    "competitor_price", "competitor_markup",
    "current_stock", "days_of_supply",
    "units_sold_7d", "units_sold_30d", "avg_daily_units", "seasonality_index",
    "view_count", "cart_add_rate", "conversion_rate", "return_rate",
    "discount_pct",
    "cost_delta_pct",
    "ppi_steel", "ppi_copper", "ppi_pvc",
]
TARGET_COL = "optimal_price"

X = pdf[FEATURE_COLS].fillna(0)
y = pdf[TARGET_COL]

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
print(f"Train: {len(X_train):,} | Test: {len(X_test):,}")

# Set MLflow tracking to Unity Catalog experiment
mlflow.set_registry_uri("databricks-uc")
mlflow.set_experiment(f"/Users/{spark.sql('SELECT current_user()').first()[0]}/pricing_model_demo")

# Enable autolog — captures params, metrics, feature importance, model artifact
mlflow.lightgbm.autolog(log_models=True)

params = {
    "objective":       "regression",
    "metric":          "mae",
    "num_leaves":      63,
    "learning_rate":   0.05,
    "n_estimators":    300,
    "feature_fraction":0.8,
    "bagging_fraction":0.8,
    "bagging_freq":    5,
    "verbose":         -1,
}

with mlflow.start_run(run_name="pricing_lgbm_v1") as run:
    model = lgb.LGBMRegressor(**params)
    model.fit(
        X_train, y_train,
        eval_set=[(X_test, y_test)],
        callbacks=[lgb.early_stopping(30, verbose=False)],
    )

    # Evaluate
    y_pred = model.predict(X_test)
    mae  = mean_absolute_error(y_test, y_pred)
    r2   = r2_score(y_test, y_pred)
    mape = float(np.mean(np.abs((y_test - y_pred) / y_test.clip(lower=0.01))) * 100)

    mlflow.log_metrics({"test_mae": mae, "test_r2": r2, "test_mape_pct": mape})

    # Log feature columns for serving reference
    mlflow.log_param("feature_cols", ",".join(FEATURE_COLS))

    run_id = run.info.run_id
    print(f"\nRun ID:  {run_id}")
    print(f"MAE:     ${mae:.4f}")
    print(f"R²:      {r2:.4f}")
    print(f"MAPE:    {mape:.2f}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 · Register Model to Unity Catalog

# COMMAND ----------

model_uri = f"runs:/{run_id}/model"

registered = mlflow.register_model(
    model_uri=model_uri,
    name=model_name,
)

print(f"Registered: {model_name} v{registered.version}")

# Add description and alias
client = mlflow.tracking.MlflowClient()
client.update_registered_model(
    name=model_name,
    description="LightGBM price recommendation model. Inputs: 19 SKU features from gold_feature_mart. Output: optimal_price (USD).",
)
client.set_registered_model_alias(model_name, "champion", registered.version)
print(f"Alias 'champion' → version {registered.version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 · Deploy Serverless Model Serving Endpoint

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import (
    EndpointCoreConfigInput,
    ServedEntityInput,
    TrafficConfig,
    Route,
)

w = WorkspaceClient()

endpoint_config = EndpointCoreConfigInput(
    name=endpoint_name,
    served_entities=[
        ServedEntityInput(
            name="pricing-model-champion",
            entity_name=model_name,
            entity_version=str(registered.version),
            workload_size="Small",
            scale_to_zero_enabled=True,
        )
    ],
    traffic_config=TrafficConfig(
        routes=[Route(served_model_name="pricing-model-champion", traffic_percentage=100)]
    ),
)

# Create or update endpoint
existing = [e.name for e in w.serving_endpoints.list()]

if endpoint_name in existing:
    print(f"Updating existing endpoint: {endpoint_name}")
    w.serving_endpoints.update_config_and_wait(name=endpoint_name, served_entities=endpoint_config.served_entities)
else:
    print(f"Creating endpoint: {endpoint_name}")
    w.serving_endpoints.create_and_wait(name=endpoint_name, config=endpoint_config)

print(f"✓ Endpoint '{endpoint_name}' is READY")
print(f"\nNext step: Run '04_pricing_endpoint_demo' to see recommendations")
