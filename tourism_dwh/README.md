# Tourism DW (Postgres + pgAdmin)

Minimal Docker setup that spins up:
- PostgreSQL 16 with star schema for EMTA × VisitEstonia
- Mock data (companies, properties, features, quarters)
- pgAdmin UI to inspect/query

## 1) Prereqs
- Docker Desktop (or Docker Engine + Compose)

## 2) Configure
Create `.env` in the repo root (values can stay as-is for local dev):
```
POSTGRES_USER=data_engineer
POSTGRES_PASSWORD=Pass!w0rd
POSTGRES_DB=assignment
PGADMIN_DEFAULT_EMAIL=admin@admin.com

PGADMIN_DEFAULT_PASSWORD=root
```


## 3) Start
From repo root:
```bash
docker compose up -d
```

* Postgres: localhost:5432 (DB: assignment)
* pgAdmin: http://localhost:5050
 (login with .env creds)

>The first run initializes the database and executes all pg_init/*.sql files.

## 4) Connect pgAdmin to the database

1. Open pgAdmin (http://localhost:5050
)

2. Add New Server
   * General → Name: local-db
   * Connection → Host name/address: db
   * Port: 5432
   * Maintenance DB: assignment
   * Username: data_engineer
   * Password: Pass!w0rd

3. Save. Expand Databases → assignment → Schemas → tourism_dw.

## 5) Try a query

In pgAdmin → Query Tool:
```
SET search_path TO tourism_dw;

-- Sample KPI: turnover per bed (modeled) by county, latest quarter
WITH cap AS (
  SELECT quarter_sk, company_sk, geo_sk, SUM(beds_total) AS beds
  FROM fact_accommodation_snapshot
  GROUP BY 1,2,3
),
latest_q AS (SELECT MAX(quarter_sk) AS q FROM dim_calendar_quarter)
SELECT g.county,
       SUM(f.turnover_eur) / NULLIF(SUM(cap.beds),0) AS eur_per_bed
FROM fact_company_quarter f
JOIN cap ON cap.company_sk = f.company_sk AND cap.quarter_sk = f.quarter_sk
JOIN dim_geography g ON f.geo_sk = g.geo_sk
WHERE f.quarter_sk = (SELECT q FROM latest_q)
GROUP BY g.county
ORDER BY eur_per_bed DESC;
```