# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 00 · Raw Source Data Generator
# MAGIC
# MAGIC **Purpose**: Seeds all raw Delta source tables for the Online Hardware Retailer Pricing Demo.
# MAGIC
# MAGIC Simulates a real retailer's operational databases as the "message bus" layer:
# MAGIC
# MAGIC | Table | Description | Rows |
# MAGIC |-------|-------------|------|
# MAGIC | `raw.product_catalog` | SKU master data | ~500 |
# MAGIC | `raw.inventory_events` | Stock level change events | ~15,000 |
# MAGIC | `raw.competitor_prices` | Web-scraped competitor pricing | ~25,000 |
# MAGIC | `raw.daily_sales` | Historical daily units sold | ~90,000 |
# MAGIC | `raw.customer_behavior` | View/cart/conversion metrics | ~500 |
# MAGIC | `raw.promotions` | Active & historical discounts | ~2,000 |
# MAGIC | `raw.supplier_cost_changes` | Cost delta events from suppliers | ~5,000 |
# MAGIC | `raw.macro_signals` | Weekly PPI indices (steel/copper/PVC) | ~520 |
# MAGIC
# MAGIC **Run once** before starting the demo pipeline sequence.

# COMMAND ----------

# MAGIC %pip install faker --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# Widget parameters (set by DABs job or notebook defaults)
dbutils.widgets.text("catalog", "jsp_demo_trove_catalog", "Catalog")
dbutils.widgets.text("raw_schema", "raw", "Raw Schema")
dbutils.widgets.text("bronze_schema", "bronze", "Bronze Schema")
dbutils.widgets.text("silver_schema", "silver", "Silver Schema")
dbutils.widgets.text("gold_schema", "gold", "Gold Schema")
dbutils.widgets.text("ml_schema", "ml", "ML Schema")

catalog      = dbutils.widgets.get("catalog")
raw_schema   = dbutils.widgets.get("raw_schema")
bronze_schema = dbutils.widgets.get("bronze_schema")
silver_schema = dbutils.widgets.get("silver_schema")
gold_schema  = dbutils.widgets.get("gold_schema")
ml_schema    = dbutils.widgets.get("ml_schema")

print(f"Catalog: {catalog}")
print(f"Schemas to create: {raw_schema}, {bronze_schema}, {silver_schema}, {gold_schema}, {ml_schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 · Create Catalog Schemas + Volumes

# COMMAND ----------

for schema in [raw_schema, bronze_schema, silver_schema, gold_schema, ml_schema]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
    print(f"  ✓ {catalog}.{schema}")

# Checkpoint volumes for Structured Streaming jobs
for schema in [bronze_schema, silver_schema]:
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.checkpoints")
    print(f"  ✓ Volume: {catalog}.{schema}.checkpoints")

print("\nAll schemas and volumes ready.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 · Import Libraries + Configure Faker

# COMMAND ----------

import random
from datetime import datetime, timedelta, date

from faker import Faker
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, DateType, TimestampType, BooleanType
)

fake = Faker()
Faker.seed(42)
random.seed(42)

# Hardware retailer constants
CATEGORIES = {
    "Tools": {
        "Power Tools": ["Drill", "Circular Saw", "Jigsaw", "Sander", "Router"],
        "Hand Tools":  ["Hammer", "Screwdriver Set", "Wrench Set", "Pliers", "Chisel Set"],
        "Measuring":   ["Tape Measure", "Level", "Square", "Laser Level", "Stud Finder"],
    },
    "Fasteners": {
        "Screws":  ["Wood Screws", "Drywall Screws", "Machine Screws", "Sheet Metal Screws"],
        "Bolts":   ["Hex Bolts", "Carriage Bolts", "Lag Bolts", "Anchor Bolts"],
        "Nails":   ["Framing Nails", "Finish Nails", "Roofing Nails", "Brad Nails"],
    },
    "Electrical": {
        "Wiring":     ["Romex 12-2", "Romex 14-2", "THHN Wire", "Conduit"],
        "Devices":    ["Outlet", "Switch", "GFCI Outlet", "Dimmer Switch"],
        "Breakers":   ["15A Breaker", "20A Breaker", "30A Breaker", "AFCI Breaker"],
    },
    "Plumbing": {
        "Pipes":    ["PVC Pipe", "Copper Pipe", "PEX Tubing", "ABS Pipe"],
        "Fittings": ["Elbow", "Tee", "Coupling", "Ball Valve", "Gate Valve"],
        "Fixtures": ["Faucet", "Showerhead", "P-Trap", "Wax Ring"],
    },
    "Paint": {
        "Interior":  ["Flat White", "Eggshell Gray", "Satin Beige", "Semi-Gloss White"],
        "Exterior":  ["Exterior Flat", "Elastomeric", "Deck Stain", "Masonry Paint"],
        "Primers":   ["Drywall Primer", "Oil-Based Primer", "Shellac Primer"],
    },
    "Safety": {
        "PPE":        ["Safety Glasses", "Gloves", "Hard Hat", "Ear Protection", "Dust Mask"],
        "First Aid":  ["First Aid Kit", "Eye Wash", "Burn Gel"],
        "Fall Prot.": ["Safety Harness", "Lanyard", "Anchor Point"],
    },
}

BRANDS = {
    "Tools":      ["DeWalt", "Milwaukee", "Makita", "Stanley", "Ridgid", "Bosch", "Craftsman"],
    "Fasteners":  ["Hillman", "Simpson Strong-Tie", "Grip-Rite", "Everbilt", "Stanley"],
    "Electrical": ["Leviton", "Hubbell", "Square D", "Siemens", "Lutron", "Southwire"],
    "Plumbing":   ["SharkBite", "Watts", "Nibco", "Charlotte Pipe", "Apollo", "Moen"],
    "Paint":      ["Sherwin-Williams", "Benjamin Moore", "Behr", "Valspar", "Rust-Oleum"],
    "Safety":     ["3M", "Honeywell", "MSA Safety", "Ergodyne", "Klein Tools"],
}

COMPETITORS = ["HomeDepot", "Lowes", "AceHardware", "Menards", "TrueValue"]

print(f"Config loaded: {sum(len(v) for v in CATEGORIES.values())} subcategories across {len(CATEGORIES)} categories")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 · Generate `product_catalog` (~500 SKUs)

# COMMAND ----------

def make_sku_id(i):
    return f"SKU-{i:05d}"

def base_cost_for_category(category, subcategory):
    """Realistic cost ranges by category."""
    ranges = {
        "Tools":      {"Power Tools": (35, 180), "Hand Tools": (8, 45), "Measuring": (5, 95)},
        "Fasteners":  {"Screws": (3, 18), "Bolts": (4, 25), "Nails": (5, 22)},
        "Electrical": {"Wiring": (15, 85), "Devices": (4, 28), "Breakers": (8, 55)},
        "Plumbing":   {"Pipes": (6, 45), "Fittings": (3, 22), "Fixtures": (18, 120)},
        "Paint":      {"Interior": (14, 38), "Exterior": (16, 45), "Primers": (12, 32)},
        "Safety":     {"PPE": (3, 45), "First Aid": (8, 75), "Fall Prot.": (35, 220)},
    }
    lo, hi = ranges.get(category, {}).get(subcategory, (5, 50))
    return round(random.uniform(lo, hi), 2)

products = []
sku_counter = 1

for category, subcats in CATEGORIES.items():
    brands = BRANDS[category]
    for subcategory, product_types in subcats.items():
        for product_type in product_types:
            for size_variant in ["Standard", "Pro", "Value Pack"] if random.random() > 0.4 else ["Standard"]:
                sku_id = make_sku_id(sku_counter)
                brand = random.choice(brands)
                unit_cost = base_cost_for_category(category, subcategory)
                if size_variant == "Pro":
                    unit_cost = round(unit_cost * random.uniform(1.3, 1.8), 2)
                elif size_variant == "Value Pack":
                    unit_cost = round(unit_cost * random.uniform(1.5, 2.5), 2)

                products.append({
                    "sku_id": sku_id,
                    "product_name": f"{brand} {product_type} - {size_variant}",
                    "category": category,
                    "subcategory": subcategory,
                    "product_type": product_type,
                    "brand": brand,
                    "unit_cost": unit_cost,
                    "weight_lbs": round(random.uniform(0.1, 25.0), 2),
                    "is_active": random.random() > 0.05,
                    "lead_time_days": random.randint(3, 45),
                    "supplier_id": f"SUP-{random.randint(1, 30):03d}",
                    "created_date": (date.today() - timedelta(days=random.randint(30, 1095))).isoformat(),
                })
                sku_counter += 1

df_catalog = spark.createDataFrame(products)
df_catalog.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{raw_schema}.product_catalog")

sku_count = df_catalog.count()
print(f"✓ product_catalog: {sku_count} SKUs")
all_skus = [r.sku_id for r in df_catalog.select("sku_id").collect()]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 · Generate `inventory_events` (~15,000 events)

# COMMAND ----------

from pyspark.sql.types import LongType

inv_events = []
today = date.today()

for sku_id in all_skus:
    # Each SKU gets 20-40 inventory change events over the past 180 days
    num_events = random.randint(20, 40)
    current_stock = random.randint(50, 500)

    for _ in range(num_events):
        days_ago = random.randint(0, 180)
        event_ts = datetime.combine(today - timedelta(days=days_ago), datetime.min.time()) + \
                   timedelta(hours=random.randint(6, 22), minutes=random.randint(0, 59))

        event_type = random.choices(
            ["restock", "sale", "adjustment", "shrinkage"],
            weights=[0.3, 0.5, 0.15, 0.05]
        )[0]

        if event_type == "restock":
            delta = random.randint(50, 300)
        elif event_type == "sale":
            delta = -random.randint(1, min(20, max(1, current_stock)))
        elif event_type == "adjustment":
            delta = random.randint(-10, 10)
        else:  # shrinkage
            delta = -random.randint(1, 5)

        current_stock = max(0, current_stock + delta)

        inv_events.append({
            "event_id": fake.uuid4(),
            "sku_id": sku_id,
            "event_type": event_type,
            "stock_delta": delta,
            "stock_after": current_stock,
            "warehouse_id": f"WH-{random.randint(1, 5):02d}",
            "event_timestamp": event_ts,
        })

df_inv = spark.createDataFrame(inv_events)
df_inv.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{raw_schema}.inventory_events")
print(f"✓ inventory_events: {df_inv.count():,} events")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 · Generate `competitor_prices` (~25,000 events)

# COMMAND ----------

comp_events = []

for sku_id in all_skus:
    # Get unit_cost for this SKU from our catalog
    sku_data = next(p for p in products if p["sku_id"] == sku_id)
    unit_cost = sku_data["unit_cost"]
    market_price = round(unit_cost * random.uniform(1.2, 1.6), 2)

    # 3-5 competitor price observations per SKU over 90 days
    for competitor in random.sample(COMPETITORS, k=random.randint(3, 5)):
        num_obs = random.randint(8, 15)
        price = market_price * random.uniform(0.88, 1.12)

        for _ in range(num_obs):
            days_ago = random.randint(0, 90)
            event_ts = datetime.combine(today - timedelta(days=days_ago), datetime.min.time()) + \
                       timedelta(hours=random.randint(1, 23))

            # Drift price slightly each observation
            price = max(unit_cost * 1.05, price * random.uniform(0.97, 1.03))

            comp_events.append({
                "event_id": fake.uuid4(),
                "sku_id": sku_id,
                "competitor": competitor,
                "competitor_price": round(price, 2),
                "scrape_url": f"https://{competitor.lower()}.com/p/{sku_id}",
                "event_timestamp": event_ts,
            })

df_comp = spark.createDataFrame(comp_events)
df_comp.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{raw_schema}.competitor_prices")
print(f"✓ competitor_prices: {df_comp.count():,} events")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 · Generate `daily_sales` (~90,000 rows)

# COMMAND ----------

daily_sales = []
start_date = today - timedelta(days=365)

for sku_id in all_skus:
    sku_data = next(p for p in products if p["sku_id"] == sku_id)
    category = sku_data["category"]

    # Base daily demand by category
    base_demand = {
        "Tools": random.uniform(1, 8),
        "Fasteners": random.uniform(5, 40),
        "Electrical": random.uniform(2, 15),
        "Plumbing": random.uniform(2, 12),
        "Paint": random.uniform(1, 10),
        "Safety": random.uniform(1, 6),
    }.get(category, 3)

    for day_offset in range(180):  # 180 days of history
        sale_date = start_date + timedelta(days=day_offset)
        dow = sale_date.weekday()  # 0=Mon, 6=Sun

        # Weekend uplift for DIY categories
        weekend_factor = 1.4 if dow >= 5 and category in ["Tools", "Paint"] else 1.0
        # Seasonal index (spring/summer higher for home improvement)
        month = sale_date.month
        seasonal = 1.3 if month in [4, 5, 6, 7] else (0.85 if month in [11, 12, 1] else 1.0)

        units = max(0, int(base_demand * weekend_factor * seasonal * random.uniform(0.5, 1.5)))

        if units > 0:
            daily_sales.append({
                "sku_id": sku_id,
                "sale_date": sale_date,
                "units_sold": units,
                "revenue": round(units * sku_data["unit_cost"] * random.uniform(1.25, 1.55), 2),
                "store_id": f"STORE-{random.randint(1, 12):02d}",
                "seasonality_index": round(seasonal, 2),
            })

df_sales = spark.createDataFrame(daily_sales)
df_sales.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{raw_schema}.daily_sales")
print(f"✓ daily_sales: {df_sales.count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7 · Generate `customer_behavior`

# COMMAND ----------

behavior_rows = []

for sku_id in all_skus:
    sku_data = next(p for p in products if p["sku_id"] == sku_id)
    # Higher-cost items get more views but lower conversion
    price_tier = "high" if sku_data["unit_cost"] > 50 else ("mid" if sku_data["unit_cost"] > 15 else "low")

    view_count = random.randint(50, 5000) if price_tier == "low" else random.randint(20, 1500)
    cart_add_rate = random.uniform(0.05, 0.25) if price_tier == "high" else random.uniform(0.1, 0.45)
    conversion_rate = cart_add_rate * random.uniform(0.3, 0.7)

    behavior_rows.append({
        "sku_id": sku_id,
        "view_count": view_count,
        "cart_add_rate": round(cart_add_rate, 4),
        "conversion_rate": round(conversion_rate, 4),
        "return_rate": round(random.uniform(0.01, 0.12), 4),
        "avg_session_time_sec": random.randint(15, 300),
        "snapshot_date": today,
    })

df_behavior = spark.createDataFrame(behavior_rows)
df_behavior.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{raw_schema}.customer_behavior")
print(f"✓ customer_behavior: {df_behavior.count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8 · Generate `promotions`

# COMMAND ----------

promo_rows = []

for sku_id in random.sample(all_skus, k=int(len(all_skus) * 0.4)):  # 40% of SKUs have promos
    num_promos = random.randint(1, 5)
    for i in range(num_promos):
        days_ago_start = random.randint(-7, 60)  # Some promos start in the future
        start_date_p = today - timedelta(days=days_ago_start)
        end_date_p = start_date_p + timedelta(days=random.randint(7, 30))

        promo_rows.append({
            "promo_id": fake.uuid4(),
            "sku_id": sku_id,
            "promo_type": random.choice(["CLEARANCE", "SEASONAL", "COMPETITIVE", "VOLUME", "BUNDLE"]),
            "discount_pct": round(random.uniform(0.05, 0.35), 4),
            "start_date": start_date_p,
            "end_date": end_date_p,
            "is_active": start_date_p <= today <= end_date_p,
            "created_by": fake.name(),
        })

df_promos = spark.createDataFrame(promo_rows)
df_promos.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{raw_schema}.promotions")
print(f"✓ promotions: {df_promos.count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9 · Generate `supplier_cost_changes`

# COMMAND ----------

supplier_events = []

for sku_id in all_skus:
    num_changes = random.randint(3, 15)
    for _ in range(num_changes):
        days_ago = random.randint(0, 365)
        event_ts = datetime.combine(today - timedelta(days=days_ago), datetime.min.time()) + \
                   timedelta(hours=random.randint(8, 17))

        supplier_events.append({
            "event_id": fake.uuid4(),
            "sku_id": sku_id,
            "supplier_id": next(p for p in products if p["sku_id"] == sku_id)["supplier_id"],
            "cost_delta_pct": round(random.uniform(-0.08, 0.15), 4),  # -8% to +15%
            "reason": random.choice(["RAW_MATERIAL", "TARIFF", "FUEL_SURCHARGE", "VOLUME_DISCOUNT", "NEGOTIATION"]),
            "event_timestamp": event_ts,
        })

df_supplier = spark.createDataFrame(supplier_events)
df_supplier.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{raw_schema}.supplier_cost_changes")
print(f"✓ supplier_cost_changes: {df_supplier.count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10 · Generate `macro_signals` (Weekly PPI)

# COMMAND ----------

macro_rows = []
start_week = today - timedelta(weeks=520)  # 10 years of weekly data

steel_ppi  = 100.0
copper_ppi = 100.0
pvc_ppi    = 100.0

for week_offset in range(520):
    signal_week = start_week + timedelta(weeks=week_offset)

    # Random walk with mean reversion
    steel_ppi  = max(60, min(200, steel_ppi  * random.uniform(0.985, 1.015)))
    copper_ppi = max(55, min(220, copper_ppi * random.uniform(0.982, 1.018)))
    pvc_ppi    = max(70, min(180, pvc_ppi    * random.uniform(0.988, 1.012)))

    macro_rows.append({
        "signal_week": signal_week,
        "ppi_steel":  round(steel_ppi, 2),
        "ppi_copper": round(copper_ppi, 2),
        "ppi_pvc":    round(pvc_ppi, 2),
        "source": "BLS",  # Bureau of Labor Statistics
    })

df_macro = spark.createDataFrame(macro_rows)
df_macro.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{raw_schema}.macro_signals")
print(f"✓ macro_signals: {df_macro.count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 60)
print("  RAW SOURCE TABLES — GENERATION COMPLETE")
print("=" * 60)
tables = [
    "product_catalog", "inventory_events", "competitor_prices",
    "daily_sales", "customer_behavior", "promotions",
    "supplier_cost_changes", "macro_signals"
]
for t in tables:
    count = spark.table(f"{catalog}.{raw_schema}.{t}").count()
    print(f"  ✓ {catalog}.{raw_schema}.{t:<30} {count:>8,} rows")

print("=" * 60)
print("\nNext step: Run Job '01-02 - Bronze + Silver Streaming Ingest'")
