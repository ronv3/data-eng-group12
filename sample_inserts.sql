-- =========================================================
-- Minimal mock data for EMTA × VisitEstonia star schema
-- =========================================================
-- Goal: tiny but realistic dataset to exercise joins & queries
-- - 2 companies (one with 2 properties; one with 1)
-- - 1 property without a company (no registry code)
-- - 2 counties (Saare, Harju) + regions
-- - 4 quarters (2024 Q4 → 2025 Q3)
-- - Features with M:N bridge
-- =========================================================

-- -------------------------
-- Calendar (quarters)
-- -------------------------
INSERT INTO dim_calendar_quarter (year, quarter, quarter_start, quarter_end)
VALUES
 (2024, 4, DATE '2024-10-01', DATE '2024-12-31'),
 (2025, 1, DATE '2025-01-01', DATE '2025-03-31'),
 (2025, 2, DATE '2025-04-01', DATE '2025-06-30'),
 (2025, 3, DATE '2025-07-01', DATE '2025-09-30');

-- -------------------------
-- Geography
-- -------------------------
INSERT INTO dim_geography (region, county, municipality, island) VALUES
 ('Lääne-Eesti', 'Saare', 'Saaremaa vald', 'Saaremaa'),
 ('Põhja-Eesti', 'Harju', 'Viimsi vald', NULL);

-- -------------------------
-- Companies (SCD2 current rows)
-- registry_code is the business key
-- -------------------------
INSERT INTO dim_company
(registry_code, name, activity, website, email, phone, address, municipality, county,
 effective_from, effective_to, is_current)
VALUES
 ('11460940', 'LILJE OÜ', 'ACCOMMODATION AND FOOD SERVICE ACTIVITIES', NULL, NULL, NULL,
  'Muhu vald, Saare maakond', 'Muhu vald', 'Saare', DATE '2024-01-01', NULL, TRUE),
 ('12727115', 'RANNAKÜLA PUHKEMAJA OÜ', 'ACCOMMODATION AND FOOD SERVICE ACTIVITIES', NULL, 'gert@sky.ee', '+3725129456',
  'Karikakra tee 9, Püünsi küla', 'Viimsi vald', 'Harju', DATE '2024-01-01', NULL, TRUE);

-- -------------------------
-- Accommodation (SCD2 current rows)
-- property_bk is a synthetic business key (e.g., hash(normalized(name,address))
-- -------------------------
INSERT INTO dim_accommodation
(property_bk, name, category, type, stars, booking_link, socials, seasonal_flag,
 effective_from, effective_to, is_current)
VALUES
 ('bk_lilje_guesthouse_muhu',  'Lilje Guesthouse',     'Majutus', 'Puhkemaja', 0, NULL, NULL, FALSE, DATE '2024-01-01', NULL, TRUE),
 ('bk_saare_cabin_muhu',       'Saare Seaside Cabin',  'Majutus', 'Puhkemaja', 0, NULL, NULL, TRUE,  DATE '2024-01-01', NULL, TRUE),
 ('bk_puhka_monuga_viimsi',    'Puhka mönuga',         'Majutus', 'Puhkemaja', 0, 'https://airbnb.example/11595675', NULL, FALSE, DATE '2024-01-01', NULL, TRUE),
 -- property without company registry code (will remain un-joined to EMTA)
 ('bk_orphan_forest_hut',      'Forest Hut',           'Majutus', 'Puhkemaja', 0, NULL, NULL, TRUE,  DATE '2024-01-01', NULL, TRUE);

-- -------------------------
-- Features (dictionary) and bridge
-- -------------------------
INSERT INTO dim_feature (feature_name) VALUES
 ('WIFI'), ('Parking'), ('Laundry'), ('Fridge');

-- Map features to accommodations
-- Lilje Guesthouse: WIFI, Parking
INSERT INTO bridge_accommodation_feature (accommodation_sk, feature_sk)
SELECT a.accommodation_sk, f.feature_sk
FROM dim_accommodation a
JOIN dim_feature f ON f.feature_name IN ('WIFI', 'Parking')
WHERE a.property_bk = 'bk_lilje_guesthouse_muhu';

-- Saare Seaside Cabin: WIFI, Fridge
INSERT INTO bridge_accommodation_feature (accommodation_sk, feature_sk)
SELECT a.accommodation_sk, f.feature_sk
FROM dim_accommodation a
JOIN dim_feature f ON f.feature_name IN ('WIFI', 'Fridge')
WHERE a.property_bk = 'bk_saare_cabin_muhu';

-- Puhka mönuga (Viimsi): WIFI, Laundry, Parking
INSERT INTO bridge_accommodation_feature (accommodation_sk, feature_sk)
SELECT a.accommodation_sk, f.feature_sk
FROM dim_accommodation a
JOIN dim_feature f ON f.feature_name IN ('WIFI', 'Laundry', 'Parking')
WHERE a.property_bk = 'bk_puhka_monuga_viimsi';

-- Forest Hut (orphan): Fridge
INSERT INTO bridge_accommodation_feature (accommodation_sk, feature_sk)
SELECT a.accommodation_sk, f.feature_sk
FROM dim_accommodation a
JOIN dim_feature f ON f.feature_name IN ('Fridge')
WHERE a.property_bk = 'bk_orphan_forest_hut';

-- -------------------------
-- FACT: company × quarter (EMTA-like numbers)
-- - LILJE OÜ (Saare)
-- - RANNAKÜLA PUHKEMAJA OÜ (Harju)
-- -------------------------
-- 2025 Q2/Q3 show some variation; employees are quarter-end counts
INSERT INTO fact_company_quarter
(company_sk, quarter_sk, geo_sk, turnover_eur, state_taxes_eur, labour_taxes_eur, employees_cnt)
SELECT c.company_sk,
       q.quarter_sk,
       g.geo_sk,
       v.turnover_eur,
       v.state_taxes_eur,
       v.labour_taxes_eur,
       v.employees_cnt
FROM (
  VALUES
  -- registry_code, year, quarter, county, turnover, state_tax, labour_tax, employees
  ('11460940', 2024, 4, 'Saare', 11804.00, 1148.00, 101.00, 2),
  ('11460940', 2025, 1, 'Saare',  8619.00,  364.00,   0.00, 2),
  ('11460940', 2025, 2, 'Saare', 27404.00,    0.00,  54.00, 3),
  ('11460940', 2025, 3, 'Saare',  8242.00,  183.00,   0.00, 2),

  ('12727115', 2025, 2, 'Harju', 18000.00,  750.00, 320.00, 2),
  ('12727115', 2025, 3, 'Harju', 22000.00,  900.00, 360.00, 3)
) AS v(registry_code, y, qtr, county_name, turnover_eur, state_taxes_eur, labour_taxes_eur, employees_cnt)
JOIN dim_company c ON c.registry_code = v.registry_code AND c.is_current
JOIN dim_calendar_quarter q ON q.year = v.y AND q.quarter = v.qtr
JOIN dim_geography g ON g.county = v.county_name;

-- -------------------------
-- FACT: accommodation × quarter snapshot
-- - Link 2 properties to LILJE (Saare)
-- - Link 1 property to RANNAKÜLA (Harju)
-- - Add 1 orphan property (no company_sk)
-- -------------------------

-- Helper: upsert a small temp mapping (property_bk → company registry_code, county)
-- (purely to keep the INSERT..SELECT readable)
WITH map(property_bk, registry_code, county_name, rooms, beds, beds_hi, beds_lo, caravan, tent) AS (
  VALUES
    ('bk_lilje_guesthouse_muhu', '11460940', 'Saare', 3,  6,  6,  6, 0, 0),
    ('bk_saare_cabin_muhu',      '11460940', 'Saare', 2,  4,  6,  2, 0, 0),
    ('bk_puhka_monuga_viimsi',   '12727115', 'Harju', 3,  6,  6,  6, 0, 0),
    ('bk_orphan_forest_hut',     NULL,       'Saare', 1,  2,  4,  2, 0, 0)
)
INSERT INTO fact_accommodation_snapshot
(accommodation_sk, company_sk, quarter_sk, geo_sk,
 rooms_cnt, beds_total, beds_high_season, beds_low_season, caravan_spots, tent_spots)
SELECT
  a.accommodation_sk,
  c.company_sk,
  q.quarter_sk,
  g.geo_sk,
  m.rooms, m.beds, m.beds_hi, m.beds_lo, m.caravan, m.tent
FROM map m
JOIN dim_accommodation a ON a.property_bk = m.property_bk AND a.is_current
LEFT JOIN dim_company c ON m.registry_code IS NOT NULL
                       AND c.registry_code = m.registry_code AND c.is_current
JOIN dim_geography g ON g.county = m.county_name
JOIN dim_calendar_quarter q ON (q.year, q.quarter) IN ((2025,2),(2025,3));  -- make two snapshots (Q2, Q3)
