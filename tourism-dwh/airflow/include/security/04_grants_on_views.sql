-- Full analysts get everything in gold (includes views)
GRANT SELECT ON default_gold.* TO analyst_full;

-- Ensure limited analysts cannot touch the raw sensitive table
REVOKE SELECT ON default_gold.dim_company FROM analyst_limited;

-- Both roles must be able to read the masked physical table that powers the limited view
GRANT SELECT ON default_gold_pub.dim_company_limited TO analyst_limited;
GRANT SELECT ON default_gold_pub.dim_company_limited TO analyst_full;

-- Limited analysts: allow only masked company data + safe gold objects
GRANT SELECT ON default_gold.v_dim_company_limited  TO analyst_limited;

GRANT SELECT ON default_gold.dim_feature                  TO analyst_limited;
GRANT SELECT ON default_gold.dim_geography                TO analyst_limited;
GRANT SELECT ON default_gold.dim_calendar_quarter         TO analyst_limited;
GRANT SELECT ON default_gold.bridge_accommodation_feature TO analyst_limited;
GRANT SELECT ON default_gold.fact_accommodation_snapshot  TO analyst_limited;
GRANT SELECT ON default_gold.fact_company_quarter         TO analyst_limited;
