# Tourism DW (Airflow + DBT + ClickHouse)


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

### For first run, use:
```
docker compose exec airflow-scheduler bash -lc '
  set -euo pipefail
  export PATH="$PATH:/home/airflow/.local/bin"
  export DBT_PROFILES_DIR=/opt/airflow/dbt
  cd /opt/airflow/dbt
  dbt build --select +path:models/gold --full-refresh --threads 1
'
'
```

### For future runs, use:
```
docker compose exec airflow-scheduler bash -lc '
  set -euo pipefail
  export DBT_PROFILES_DIR=/opt/airflow/dbt
  cd /opt/airflow/dbt
  dbt run --select fact_company_quarter --full-refresh --vars "use_latest_company: false" --threads 1
'
```

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