-- Company (full)
CREATE OR REPLACE VIEW default_gold.v_dim_company_full AS
SELECT *
FROM default_gold.dim_company;

-- Accommodation (full)
CREATE OR REPLACE VIEW default_gold.v_dim_accommodation_full AS
SELECT *
FROM default_gold.dim_accommodation;

-- Facts (full)
CREATE OR REPLACE VIEW default_gold.v_fact_company_quarter_full AS
SELECT *
FROM default_gold.fact_company_quarter;

CREATE OR REPLACE VIEW default_gold.v_fact_accommodation_snapshot_full AS
SELECT *
FROM default_gold.fact_accommodation_snapshot;
