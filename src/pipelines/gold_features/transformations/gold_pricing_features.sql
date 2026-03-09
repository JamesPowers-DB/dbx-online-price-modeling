-- Gold Pricing Features: Streaming Table
-- Layer: Silver → Gold
-- Pattern: Spark Declarative Pipeline (SQL)
--
-- Joins the silver competitor_prices stream with static raw reference tables
-- to produce a rich, ML-ready feature record for each pricing event.
--
-- Pipeline writes to: {catalog}.gold.gold_pricing_features
-- Pipeline reads from: silver.competitor_prices (streaming), raw.* (static lookups)

CREATE OR REFRESH STREAMING TABLE gold_pricing_features
  CLUSTER BY (sku_id)
  COMMENT "Consolidated pricing features per SKU — streaming table joining competitor prices, inventory, demand signals, and macro indicators"
AS
SELECT
  -- Identity
  comp.sku_id,
  p.product_name,
  p.category,
  p.subcategory,
  p.brand,
  p.supplier_id,

  -- Cost basis
  p.unit_cost,
  p.weight_lbs,

  -- Competitor signal
  comp.competitor,
  comp.competitor_price,
  ROUND(comp.competitor_price / NULLIF(p.unit_cost, 0), 4) AS competitor_markup,

  -- Inventory position (latest stock snapshot per SKU from silver)
  COALESCE(inv_latest.current_stock, 0)        AS current_stock,
  COALESCE(inv_latest.days_of_supply, -1)      AS days_of_supply,

  -- 7-day and 30-day rolling demand
  COALESCE(demand.units_sold_7d,  0)           AS units_sold_7d,
  COALESCE(demand.units_sold_30d, 0)           AS units_sold_30d,
  COALESCE(demand.avg_daily_units, 0)          AS avg_daily_units,
  COALESCE(demand.seasonality_index, 1.0)      AS seasonality_index,

  -- Customer engagement
  COALESCE(cb.view_count, 0)                   AS view_count,
  COALESCE(cb.cart_add_rate, 0.0)              AS cart_add_rate,
  COALESCE(cb.conversion_rate, 0.0)            AS conversion_rate,
  COALESCE(cb.return_rate, 0.0)                AS return_rate,

  -- Active promotions
  COALESCE(pr.discount_pct, 0.0)               AS discount_pct,
  COALESCE(pr.promo_type, 'NONE')              AS promo_type,

  -- Supplier cost pressure
  COALESCE(sc.cost_delta_pct, 0.0)             AS cost_delta_pct,

  -- Macro commodity indices (most recent 30-day average)
  COALESCE(mac.ppi_steel,  100.0)              AS ppi_steel,
  COALESCE(mac.ppi_copper, 100.0)              AS ppi_copper,
  COALESCE(mac.ppi_pvc,    100.0)              AS ppi_pvc,

  -- Simulated current (shelf) price = cost * 1.35 markup
  ROUND(p.unit_cost * 1.35, 2)                 AS current_price,

  -- Metadata
  comp.event_timestamp                         AS source_event_timestamp,
  current_timestamp()                          AS _updated_at

FROM STREAM(silver.competitor_prices) AS comp

-- Product master (static)
INNER JOIN raw.product_catalog AS p
  ON comp.sku_id = p.sku_id

-- Latest inventory per SKU (latest-per-key from silver inventory)
LEFT JOIN (
  SELECT sku_id, current_stock, days_of_supply
  FROM (
    SELECT *,
      ROW_NUMBER() OVER (PARTITION BY sku_id ORDER BY event_timestamp DESC) AS rn
    FROM silver.inventory
  )
  WHERE rn = 1
) AS inv_latest
  ON comp.sku_id = inv_latest.sku_id

-- Rolling demand signals (7d and 30d from raw daily_sales)
LEFT JOIN (
  SELECT
    sku_id,
    SUM(units_sold) FILTER (WHERE sale_date >= CURRENT_DATE() - INTERVAL 7 DAYS) AS units_sold_7d,
    SUM(units_sold) FILTER (WHERE sale_date >= CURRENT_DATE() - INTERVAL 30 DAYS) AS units_sold_30d,
    AVG(units_sold) FILTER (WHERE sale_date >= CURRENT_DATE() - INTERVAL 30 DAYS) AS avg_daily_units,
    AVG(seasonality_index) AS seasonality_index
  FROM raw.daily_sales
  WHERE sale_date >= CURRENT_DATE() - INTERVAL 30 DAYS
  GROUP BY sku_id
) AS demand
  ON comp.sku_id = demand.sku_id

-- Customer behavior (static snapshot)
LEFT JOIN raw.customer_behavior AS cb
  ON comp.sku_id = cb.sku_id

-- Active promotions (latest active promo per SKU)
LEFT JOIN (
  SELECT sku_id, discount_pct, promo_type
  FROM (
    SELECT *,
      ROW_NUMBER() OVER (PARTITION BY sku_id ORDER BY discount_pct DESC) AS rn
    FROM raw.promotions
    WHERE is_active = TRUE
  )
  WHERE rn = 1
) AS pr
  ON comp.sku_id = pr.sku_id

-- Supplier cost pressure (avg delta over last 90 days)
LEFT JOIN (
  SELECT sku_id, AVG(cost_delta_pct) AS cost_delta_pct
  FROM raw.supplier_cost_changes
  WHERE event_timestamp >= CURRENT_TIMESTAMP() - INTERVAL 90 DAYS
  GROUP BY sku_id
) AS sc
  ON comp.sku_id = sc.sku_id

-- Macro commodity PPI (trailing 4-week average)
LEFT JOIN (
  SELECT
    AVG(ppi_steel)  AS ppi_steel,
    AVG(ppi_copper) AS ppi_copper,
    AVG(ppi_pvc)    AS ppi_pvc
  FROM raw.macro_signals
  WHERE signal_week >= CURRENT_DATE() - INTERVAL 28 DAYS
) AS mac
  ON TRUE
