SET search_path TO tourism_dw;

-- SCD2-friendly “only one current row per BK” (partial unique indexes)
CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_company_bk_current
  ON dim_company (registry_code) WHERE is_current;

CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_accommodation_bk_current
  ON dim_accommodation (property_bk) WHERE is_current;

COMMENT ON TABLE fact_company_quarter IS
  'Grain: company × quarter. Measures from EMTA (company-level only).';

COMMENT ON TABLE fact_accommodation_snapshot IS
  'Grain: accommodation × quarter (snapshot). No turnover stored here (company-level only).';
