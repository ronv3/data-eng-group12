# Tourism DW (Postgres + pgAdmin)

Minimal Docker setup that spins up:
- PostgreSQL 16 with star schema for EMTA × VisitEstonia
- Mock data (companies, properties, features, quarters)
- pgAdmin UI to inspect/query

## 1) Prereqs
- Docker Desktop (or Docker Engine + Compose)

## 2) Configure
Modify `.env` in the repo root (or values can stay as-is for local dev):
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

## 4) Connect pgAdmin to the database (first time run)

1. Connect with pgAdmin:
    *   Open your web browser and go to `http://localhost:5050`.
    *   Log in using the email (`admin@admin.com`) and password (`root`) from your `.env` file.
    *   To connect to your database:
        1.  Right-click `Servers` -> `Create` -> `Server...`.
        2.  **General tab**: Give it a name (e.g., tourism-db).
        3.  **Connection tab**:
            *   **Host name/address**: `db-tourism` (This is the service name from `compose.yml`).
            *   **Port**: `5432`.
            *   **Maintenance database**: `assignment`.
            *   **Username**: `data_engineer`.
            *   **Password**: `Pass!w0rd`.
        4.  Click `Save`. You should now be connected to your PostgreSQL database!

2. Save. Expand Databases → assignment → Schemas → tourism_dwh.

## 5) Try a query

In pgAdmin → Query Tool:

### Sample KPI queries:

1. Turnover per bed (modeled) by county, latest quarter
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

2. Turnover per employee
```
SELECT 
    da.type AS accommodation_type,
    SUM(fcq.turnover_eur) / NULLIF(SUM(fcq.employees_cnt), 0) AS turnover_per_employee
FROM fact_company_quarter fcq
JOIN dim_company dc 
    ON dc.company_sk = fcq.company_sk
JOIN fact_accommodation_snapshot fas 
    ON fas.company_sk = dc.company_sk
JOIN dim_accommodation da 
    ON da.accommodation_sk = fas.accommodation_sk
WHERE fcq.employees_cnt IS NOT NULL
GROUP BY da.type
ORDER BY turnover_per_employee DESC;
```