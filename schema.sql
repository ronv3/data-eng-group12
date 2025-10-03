-- =========================================================
-- Star schema for EMTA × VisitEstonia (PostgreSQL)
-- =========================================================
-- Conventions:
-- - Surrogate keys: integer IDENTITY
-- - Business keys: UNIQUE where applicable
-- - Facts use composite PKs at declared grain
-- - Basic data quality CHECKs for non-negative metrics
-- =========================================================

-- ---------- Dimensions ----------

CREATE TABLE IF NOT EXISTS dim_company (
  company_sk        INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  registry_code     VARCHAR NOT NULL UNIQUE, -- Business key
  name              TEXT,
  activity          TEXT,
  website           TEXT,
  email             TEXT,
  phone             TEXT,
  address           TEXT,
  municipality      TEXT,
  county            TEXT,
  effective_from    DATE,
  effective_to      DATE,
  is_current        BOOLEAN DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS dim_accommodation (
  accommodation_sk  INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  property_bk       VARCHAR NOT NULL UNIQUE, -- Business key: hash(normalized(name,address))
  name              TEXT,
  category          TEXT,
  type              TEXT,
  stars             SMALLINT,
  booking_link      TEXT,
  socials           TEXT,
  seasonal_flag     BOOLEAN,
  effective_from    DATE,
  effective_to      DATE,
  is_current        BOOLEAN DEFAULT TRUE,
  CONSTRAINT chk_stars_nonneg CHECK (stars IS NULL OR stars >= 0)
);

CREATE TABLE IF NOT EXISTS dim_feature (
  feature_sk        INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  feature_name      TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS bridge_accommodation_feature (
  accommodation_sk  INT NOT NULL,
  feature_sk        INT NOT NULL,
  PRIMARY KEY (accommodation_sk, feature_sk),
  FOREIGN KEY (accommodation_sk) REFERENCES dim_accommodation(accommodation_sk),
  FOREIGN KEY (feature_sk)        REFERENCES dim_feature(feature_sk)
);

CREATE TABLE IF NOT EXISTS dim_geography (
  geo_sk            INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  region            TEXT,
  county            TEXT,
  municipality      TEXT,
  island            TEXT
);

CREATE TABLE IF NOT EXISTS dim_calendar_quarter (
  quarter_sk        INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  year              SMALLINT,
  quarter           SMALLINT,
  quarter_start     DATE,
  quarter_end       DATE,
  CONSTRAINT chk_quarter_range CHECK (quarter BETWEEN 1 AND 4)
);

-- ---------- Facts ----------

CREATE TABLE IF NOT EXISTS fact_company_quarter (
  company_sk        INT NOT NULL,
  quarter_sk        INT NOT NULL,
  geo_sk            INT NOT NULL,
  turnover_eur      NUMERIC(18,2),
  state_taxes_eur   NUMERIC(18,2),
  labour_taxes_eur  NUMERIC(18,2),
  employees_cnt     INT,
  PRIMARY KEY (company_sk, quarter_sk),
  FOREIGN KEY (company_sk) REFERENCES dim_company(company_sk),
  FOREIGN KEY (quarter_sk) REFERENCES dim_calendar_quarter(quarter_sk),
  FOREIGN KEY (geo_sk)     REFERENCES dim_geography(geo_sk),
  CONSTRAINT chk_financials_nonneg CHECK (
    (turnover_eur     IS NULL OR turnover_eur     >= 0) AND
    (state_taxes_eur  IS NULL OR state_taxes_eur  >= 0) AND
    (labour_taxes_eur IS NULL OR labour_taxes_eur >= 0) AND
    (employees_cnt    IS NULL OR employees_cnt    >= 0)
  )
);

CREATE TABLE IF NOT EXISTS fact_accommodation_snapshot (
  accommodation_sk  INT NOT NULL,
  company_sk        INT,               -- nullable if no registry code
  quarter_sk        INT NOT NULL,
  geo_sk            INT NOT NULL,
  rooms_cnt         SMALLINT,
  beds_total        SMALLINT,
  beds_high_season  SMALLINT,
  beds_low_season   SMALLINT,
  caravan_spots     SMALLINT,
  tent_spots        SMALLINT,
  PRIMARY KEY (accommodation_sk, quarter_sk),
  FOREIGN KEY (accommodation_sk) REFERENCES dim_accommodation(accommodation_sk),
  FOREIGN KEY (company_sk)       REFERENCES dim_company(company_sk),
  FOREIGN KEY (quarter_sk)       REFERENCES dim_calendar_quarter(quarter_sk),
  FOREIGN KEY (geo_sk)           REFERENCES dim_geography(geo_sk),
  CONSTRAINT chk_capacity_nonneg CHECK (
    (rooms_cnt        IS NULL OR rooms_cnt        >= 0) AND
    (beds_total       IS NULL OR beds_total       >= 0) AND
    (beds_high_season IS NULL OR beds_high_season >= 0) AND
    (beds_low_season  IS NULL OR beds_low_season  >= 0) AND
    (caravan_spots    IS NULL OR caravan_spots    >= 0) AND
    (tent_spots       IS NULL OR tent_spots       >= 0)
  )
);

-- ---------- Helpful Indexes (beyond PKs) ----------

CREATE INDEX IF NOT EXISTS ix_fcq_company_sk  ON fact_company_quarter(company_sk);
CREATE INDEX IF NOT EXISTS ix_fcq_quarter_sk  ON fact_company_quarter(quarter_sk);
CREATE INDEX IF NOT EXISTS ix_fcq_geo_sk      ON fact_company_quarter(geo_sk);

CREATE INDEX IF NOT EXISTS ix_fas_company_sk  ON fact_accommodation_snapshot(company_sk);
CREATE INDEX IF NOT EXISTS ix_fas_quarter_sk  ON fact_accommodation_snapshot(quarter_sk);
CREATE INDEX IF NOT EXISTS ix_fas_geo_sk      ON fact_accommodation_snapshot(geo_sk);

-- ---------- Optional: comments for clarity ----------

COMMENT ON TABLE fact_company_quarter IS
  'Grain: company × quarter. Measures come from EMTA (company-level only).';

COMMENT ON TABLE fact_accommodation_snapshot IS
  'Grain: accommodation × quarter (snapshot from monthly feed aligned to quarter end). No turnover here.';

COMMENT ON COLUMN dim_company.registry_code IS
  'Business key (EMTA/VisitEstonia). Used for cross-source matching and SCD2 grouping.';

COMMENT ON COLUMN dim_accommodation.property_bk IS
  'Business key derived as hash(normalized(name,address)). Used to stabilize IDs across monthly updates.';