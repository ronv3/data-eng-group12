-- A tiny "published" db that only holds masked tables for limited users
CREATE DATABASE IF NOT EXISTS default_gold_pub;

-- Rebuild masked table idempotently
DROP TABLE IF EXISTS default_gold_pub.dim_company_limited;
CREATE TABLE default_gold_pub.dim_company_limited AS default_gold.dim_company;

-- Populate with masked values (3 columns: email, phone, address)
INSERT INTO default_gold_pub.dim_company_limited
SELECT
  company_sk,
  company_id,
  name,
  activity,
  website,

  /* MASK #1: email -> keep first char + domain, hide middle; keep empty/null as-is */
  multiIf(
    email IS NULL, NULL,
    email = '', '',
    replaceRegexpOne(email, '^(.).*(@.*)$', '\\1***\\2')
  ) AS email,

  /* MASK #2: phone -> replace digits with '*' ; keep empty/null as-is */
  multiIf(
    phone IS NULL, NULL,
    phone = '', '',
    replaceRegexpAll(phone, '[0-9]', '*')
  ) AS phone,

  /* MASK #3: address -> show first 3 chars, rest '*' ; keep empty/null as-is */
  multiIf(
    address IS NULL, NULL,
    address = '', '',
    concat(
      substring(address, 1, 3),
      if(length(address) > 3, repeat('*', length(address) - 3), '')
    )
  ) AS address,

  municipality,
  county,
  effective_from,
  effective_to,
  is_current
FROM default_gold.dim_company;

-- Limited view = selects from the masked table (so privileges are enforceable)
CREATE OR REPLACE VIEW default_gold.v_dim_company_limited AS
SELECT *
FROM default_gold_pub.dim_company_limited;

/* NOTE: When gold.dim_company changes and you want to refresh the masked copy:
   TRUNCATE TABLE default_gold_pub.dim_company_limited;
   INSERT INTO default_gold_pub.dim_company_limited
   SELECT ...same SELECT block as above...
*/
