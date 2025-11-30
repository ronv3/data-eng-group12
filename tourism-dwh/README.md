# Tourism DWH (Airflow + DBT + ClickHouse)


## 1) Prereqs
- Docker Desktop (or Docker Engine + Compose)

## 2) Configure
Create `.env` with the teplate of .env_template. Example credentials (feel free to choose your own):
```
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=airflow

AIRFLOW_USER=admin
AIRFLOW_PASSWORD=admin

CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=default
CLICKHOUSE_DB_BRONZE=bronze
CLICKHOUSE_DB_SILVER=silver
TZ=UTC
```

## 3) Start
From project root (tourism-dwh):
```bash
docker compose build
docker compose up -d
```

* Postgres: localhost:5432 (DB: assignment)
* Airflow: http://localhost:8080
 login with .env creds for Airflow, default credentials – Username: `admin`; Password: `admin`.

>Access ClickHouse database inside terminal with:
> ```docker exec -it clickhouse clickhouse-client```
>> Optionally you can setup DB on your IDE for more user-friendly queries

### For first run

* Run DAGs in Airflow in the right order
  * tax_quarterly_dag → iceberg_tax_bronze_dag → housing_monthly_dag → silver_dag → gold_dag

> Note: After triggering tax_quarterly_dag or housing_monthly_dag, silver and gold level DAGs are automatically queued.

#### Problems you may run into
1. `[Errno 35] Resource deadlock avoided`  (on Mac)
   * Look for the section "Choose file sharing implementation for your Docker containers". 
   * It is likely set to VirtioFS. Change it to gRPC FUSE (or osxfs if available). 
     * Note: gRPC FUSE is slightly slower but much more stable for this specific "deadlock" error.

2. Conflicting Docker Containers
   * If Docker containers with the same name are already used, disable or remove them from the list. 
   * More problems can be solved by removing previous volumes, builds, images

### For local dbt runs, use:
```
  docker compose exec airflow-scheduler bash -lc '
  set -euo pipefail
  export DBT_PROFILES_DIR=/opt/airflow/dbt
  cd /opt/airflow/dbt
  dbt run --select +path:models/gold --threads 1
'
```

> Note: `--select +path:models/gold` runs all models in `dbt/models/gold`.

## 4) Data

### We have 3 schemas – bronze, default_silver, default_gold

* Gold layer (default_gold)
>```
>   ┌─name─────────────────────────┐
>1. │ bridge_accommodation_feature │
>2. │ dim_accommodation            │
>3. │ dim_calendar_quarter         │
>4. │ dim_company                  │
>5. │ dim_feature                  │
>6. │ dim_geography                │
>7. │ fact_accommodation_snapshot  │
>8. │ fact_company_quarter         │
>   └──────────────────────────────┘
>```

* Silver layer (default_silver)
>```
>   ┌─name──────────────────────┐
>1. │ stg_company_latest        │
>2. │ stg_housing_accommodation │
>3. │ stg_housing_features      │
>4. │ stg_tax_company_quarter   │
>   └───────────────────────────┘
>```

* Bronze layer (bronze)
>```
>   ┌─name────────┐
>1. │ housing_raw │
>2. │ tax_raw     │
>   └─────────────┘
>```
