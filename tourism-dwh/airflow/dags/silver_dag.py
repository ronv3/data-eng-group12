from __future__ import annotations
import os, pendulum
from airflow import Dataset
from airflow.decorators import dag
from airflow.operators.bash import BashOperator

BRONZE_HOUSING_DS = Dataset("clickhouse://bronze/housing_raw")
BRONZE_TAX_DS     = Dataset("clickhouse://bronze/tax_raw")
SILVER_READY_DS   = Dataset("clickhouse://silver/ready")

@dag(
    dag_id="silver_dag",
    schedule=[BRONZE_HOUSING_DS, BRONZE_TAX_DS],
    start_date=pendulum.datetime(2025, 10, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["dbt","silver"]
)
def silver_dag():
    run_dbt_silver = BashOperator(
        task_id="dbt_run_and_test_silver",
        # single thread avoids CH metadata rename races and is plenty fast for silver
        bash_command=(
            'set -euo pipefail; '
            'export PATH="$PATH:/home/airflow/.local/bin"; '
            'cd /opt/airflow/dbt; '
            'dbt deps; '
            'dbt build --select path:models/silver --threads 1'
        ),
        env={
            # dbt profile and CH creds
            "DBT_PROFILES_DIR": "/opt/airflow/dbt",
            "CLICKHOUSE_HOST": os.environ.get("CLICKHOUSE_HOST", "clickhouse"),
            "CLICKHOUSE_USER": os.environ.get("CLICKHOUSE_USER", "default"),
            "CLICKHOUSE_PASSWORD": os.environ.get("CLICKHOUSE_PASSWORD", ""),
            # propagate base PATH as well (then we append user-site bin above)
            "PATH": os.environ.get("PATH", ""),
        },
        outlets=[SILVER_READY_DS]
    )

silver_dag()