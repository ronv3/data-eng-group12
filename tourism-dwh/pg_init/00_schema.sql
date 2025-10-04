-- Create dedicated schema & set search path
CREATE SCHEMA IF NOT EXISTS tourism_dwh;
SET search_path TO tourism_dwh;

-- =========================
-- Dimensions (SCD2 where noted)
-- =========================

CREATE TABLE IF NOT EXISTS dim_company (
  company_sk        INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  registry_code     VARCHAR NOT NULL,                -- Business key (no UNIQUE to allow SCD2 history)
  name              VARCHAR,
  activity          VARCHAR,
  website           VARCHAR,
  email             VARCHAR,
  phone             VARCHAR,
  address           VARCHAR,
  municipality      VARCHAR,
  county            VARCHAR,
  effective_from    DATE    NOT NULL DEFAULT DATE '1900-01-01',
  effective_to      DATE    NOT NULL DEFAULT DATE '9999-12-31',
  is_current        BOOLEAN NOT NULL DEFAULT TRUE,
  CONSTRAINT dim_company_effective_range_chk CHECK (effective_from <= effective_to)
);

CREATE TABLE IF NOT EXISTS dim_accommodation (
  accommodation_sk  INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  property_bk       VARCHAR NOT NULL,                -- BK = hash(normalized(name,address))
  name              VARCHAR,
  category          VARCHAR,
  type              VARCHAR,
  stars             SMALLINT,
  booking_link      VARCHAR,
  socials           VARCHAR,
  seasonal_flag     BOOLEAN,
  effective_from    DATE    NOT NULL DEFAULT DATE '1900-01-01',
  effective_to      DATE    NOT NULL DEFAULT DATE '9999-12-31',
  is_current        BOOLEAN NOT NULL DEFAULT TRUE,
  CONSTRAINT dim_accommodation_effective_range_chk CHECK (effective_from <= effective_to),
  CONSTRAINT dim_accommodation_stars_chk CHECK (stars IS NULL OR stars BETWEEN 0 AND 5)
);

CREATE TABLE IF NOT EXISTS dim_feature (
  feature_sk    INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  feature_name  VARCHAR NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_feature_name ON dim_feature(feature_name);

CREATE TABLE IF NOT EXISTS bridge_accommodation_feature (
  accommodation_sk INT NOT NULL,
  feature_sk       INT NOT NULL,
  PRIMARY KEY (accommodation_sk, feature_sk),
  FOREIGN KEY (accommodation_sk) REFERENCES dim_accommodation(accommodation_sk),
  FOREIGN KEY (feature_sk)       REFERENCES dim_feature(feature_sk)
);

CREATE TABLE IF NOT EXISTS dim_geography (
  geo_sk        INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  region        VARCHAR,
  county        VARCHAR,
  municipality  VARCHAR,
  island        VARCHAR
);

CREATE TABLE IF NOT EXISTS dim_calendar_quarter (
  quarter_sk     INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  year           SMALLINT NOT NULL,
  quarter        SMALLINT NOT NULL,     -- 1..4
  quarter_start  DATE    NOT NULL,
  quarter_end    DATE    NOT NULL,
  CONSTRAINT dim_cq_quarter_chk CHECK (quarter BETWEEN 1 AND 4),
  CONSTRAINT dim_cq_range_chk   CHECK (quarter_start <= quarter_end),
  CONSTRAINT dim_cq_unique UNIQUE (year, quarter)
);

-- =========================
-- Facts
-- =========================

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
  CONSTRAINT chk_fcq_nonneg CHECK (
    (turnover_eur     IS NULL OR turnover_eur     >= 0) AND
    (state_taxes_eur  IS NULL OR state_taxes_eur  >= 0) AND
    (labour_taxes_eur IS NULL OR labour_taxes_eur >= 0) AND
    (employees_cnt    IS NULL OR employees_cnt    >= 0)
  )
);

CREATE INDEX IF NOT EXISTS ix_fcq_company_sk ON fact_company_quarter(company_sk);
CREATE INDEX IF NOT EXISTS ix_fcq_quarter_sk ON fact_company_quarter(quarter_sk);
CREATE INDEX IF NOT EXISTS ix_fcq_geo_sk     ON fact_company_quarter(geo_sk);

CREATE TABLE IF NOT EXISTS fact_accommodation_snapshot (
  accommodation_sk  INT NOT NULL,
  company_sk        INT,              -- nullable if no registry code
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
  CONSTRAINT chk_fas_nonneg CHECK (
    (rooms_cnt        IS NULL OR rooms_cnt        >= 0) AND
    (beds_total       IS NULL OR beds_total       >= 0) AND
    (beds_high_season IS NULL OR beds_high_season >= 0) AND
    (beds_low_season  IS NULL OR beds_low_season  >= 0) AND
    (caravan_spots    IS NULL OR caravan_spots    >= 0) AND
    (tent_spots       IS NULL OR tent_spots       >= 0)
  )
);

CREATE INDEX IF NOT EXISTS ix_fas_company_sk ON fact_accommodation_snapshot(company_sk);
CREATE INDEX IF NOT EXISTS ix_fas_quarter_sk ON fact_accommodation_snapshot(quarter_sk);
CREATE INDEX IF NOT EXISTS ix_fas_geo_sk     ON fact_accommodation_snapshot(geo_sk);
