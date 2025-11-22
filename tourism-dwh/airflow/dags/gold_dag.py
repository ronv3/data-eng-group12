from __future__ import annotations
import os, pendulum
from airflow import Dataset
from airflow.decorators import dag
from airflow.operators.bash import BashOperator

SILVER_READY_DS = Dataset("clickhouse://silver/ready")

@dag(
    dag_id="gold_dag",
    schedule=[SILVER_READY_DS],
    start_date=pendulum.datetime(2025, 10, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["dbt","gold"]
)
def gold_dag():
    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot_dims",
        bash_command=(
            "set -euo pipefail; "
            "export PATH=\"$PATH:/home/airflow/.local/bin\"; "
            "cd /opt/airflow/dbt; "
            "dbt deps; "
            "dbt snapshot"
        ),
        env={
            "DBT_PROFILES_DIR": "/opt/airflow/dbt",
            "CLICKHOUSE_HOST": os.environ.get("CLICKHOUSE_HOST", "clickhouse"),
            "CLICKHOUSE_USER": os.environ.get("CLICKHOUSE_USER", "default"),
            "CLICKHOUSE_PASSWORD": os.environ.get("CLICKHOUSE_PASSWORD", "")
        }
    )

    dbt_run_gold = BashOperator(
        task_id="dbt_run_gold",
        bash_command=(
            "set -euo pipefail; "
            "export PATH=\"$PATH:/home/airflow/.local/bin\"; "
            "cd /opt/airflow/dbt; "
            "dbt deps; "
            "dbt build --select path:models/gold"
        ),
        env={
            "DBT_PROFILES_DIR": "/opt/airflow/dbt",
            "CLICKHOUSE_HOST": os.environ.get("CLICKHOUSE_HOST", "clickhouse"),
            "CLICKHOUSE_USER": os.environ.get("CLICKHOUSE_USER", "default"),
            "CLICKHOUSE_PASSWORD": os.environ.get("CLICKHOUSE_PASSWORD", "")
        }
    )

    dbt_test_gold = BashOperator(
        task_id="dbt_test_gold",
        bash_command=(
            "set -euo pipefail; "
            "export PATH=\"$PATH:/home/airflow/.local/bin\"; "
            "cd /opt/airflow/dbt; "
            "dbt deps; "
            "dbt test --select path:models/gold"
        ),
        env={
            "DBT_PROFILES_DIR": "/opt/airflow/dbt",
            "CLICKHOUSE_HOST": os.environ.get("CLICKHOUSE_HOST", "clickhouse"),
            "CLICKHOUSE_USER": os.environ.get("CLICKHOUSE_USER", "default"),
            "CLICKHOUSE_PASSWORD": os.environ.get("CLICKHOUSE_PASSWORD", "")
        }
    )

    dbt_snapshot >> dbt_run_gold >> dbt_test_gold

gold_dag()
