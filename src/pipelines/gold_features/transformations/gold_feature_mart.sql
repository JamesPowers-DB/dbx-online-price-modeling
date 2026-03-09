-- Gold Feature Mart: Materialized View
-- Layer: Gold Streaming Table → Gold Mart (ML-ready)
-- Pattern: Dedup to one row per SKU (latest pricing event)
--
-- This materialized view reads from gold_pricing_features (the streaming table
-- above) and collapses it to a single canonical row per SKU — the most recent
-- pricing snapshot. This is the direct input to the ML training and serving jobs.
--
-- Auto-refreshes when upstream streaming table updates.

CREATE OR REFRESH MATERIALIZED VIEW gold_feature_mart
  CLUSTER BY (sku_id)
  COMMENT "ML-ready feature mart: one row per SKU with latest pricing features. Direct input to pricing model training and serving."
AS
SELECT * EXCEPT (row_num)
FROM (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY sku_id
      ORDER BY source_event_timestamp DESC, _updated_at DESC
    ) AS row_num
  FROM gold_pricing_features
)
WHERE row_num = 1
