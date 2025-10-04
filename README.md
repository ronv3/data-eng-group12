# Accommodation Analytics — Star Schema (Project 1)

Short repo to accompany the Report.pdf. It contains a minimal star schema, a few example inserts (optional), and demo SQL answering the business questions.

## Overview
Goal: identify which locations and accommodation characteristics are most attractive for Estonian accommodation businesses, and how capacity relates to financial performance.

**Datasets**
- **EMTA** (quarterly, company-level): turnover, (labour/state) taxes, employees — joined by `registry_code`.
- **VisitEstonia / National Tourism IS — Housing** (monthly, property-level): accommodation metadata, capacity (beds/rooms), amenities, and `Ettevõtte registrikood`.

> Note: EMTA is **company-level**. We **do not** store property-level turnover. Any distribution to properties (e.g., proportional by beds) is computed in queries and labeled **modeled**.

## Files
- `schema.sql` — DDL for all dims/facts (Postgres-flavored; should also work with minor tweaks in other RDBMS).
- `demo_queries.sql` *(optional, add later)* — SELECTs that answer the business questions in the brief.
- `sample_inserts.sql` *(optional)* — a handful of rows to illustrate table shapes.

## Star Schema (tables)
- **Facts**
  - `fact_company_quarter` (grain: company × quarter) — turnover, taxes, employees.
  - `fact_accommodation_snapshot` (grain: accommodation × quarter) — capacity snapshot.
- **Dimensions**
  - `dim_company` (SCD2; BK: `registry_code`)
  - `dim_accommodation` (SCD2; BK: `property_bk` = hash(normalized(name,address)))
  - `dim_feature` + `bridge_accommodation_feature` (amenities M:N)
  - `dim_geography`
  - `dim_calendar_quarter`

## How to use
* Follow README.md instructions inside tourism.dwh

## Assumptions / Guardrails
- Some properties lack `registry_code` → capacity-only, won’t join to EMTA.
- Company legal address county may differ from property county; we show both views when relevant.
- Any property-level turnover is **modeled** in queries (never stored as ground truth).

## Licensing / Sources
Open data from EMTA and VisitEstonia/National Tourism IS. This repo contains only schema and example SQL, no redistributed datasets.

