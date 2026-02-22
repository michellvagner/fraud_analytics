# %%

from datetime import datetime, timedelta
from airflow.providers.standard.operators.bash import BashOperator

from airflow.sdk import DAG
import os

# %%

with DAG(
    dag_id="fraud_analytics",
    description="Fluxo DBT",
    start_date=datetime(2026, 2, 21),
    schedule=timedelta(days=1),
    catchup=False,
    tags=["dbt"]
) as dag:

    dbt_run = BashOperator(
        task_id="rodar_dbt",
        bash_command="source /home/vagner/repos/github/fraud_analytics/env/bin/activate && cd /home/vagner/repos/github/fraud_analytics && dbt run"
    )