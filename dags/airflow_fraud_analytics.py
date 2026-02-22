# %%

from datetime import datetime, timedelta
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from airflow.sdk import DAG
# %%
import os
home = os.environ.get("HOST_HOME", os.path.expanduser("~"))

# %%

with DAG(
    dag_id="fraud_analytics",
    description="Fluxo DBT",
    start_date=datetime(2026, 2, 21),
    schedule=timedelta(days=1),
    catchup=False,
    tags=["dbt"]
) as dag:

    dbt_run = DockerOperator(
        task_id="rodar_dbt",
        image="fraud_analytics-dbt:latest",
        command="uv run dbt run",
        auto_remove='success',
        mount_tmp_dir=False,
        docker_url="unix://var/run/docker.sock",  # socket do Docker
        network_mode="bridge",
        force_pull=False,  # usa imagem local
        mounts=[
        Mount(
            source=f"{home}/.dbt",
            target="/root/.dbt",
            type="bind"
        ),
        Mount(
            source=f"{home}/.local/share/security",
            target=f"{home}/.local/share/security",
            type="bind"
        )
    ]
    )