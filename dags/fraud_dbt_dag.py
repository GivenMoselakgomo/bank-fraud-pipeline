"""
Phase 6: Airflow orchestration for the bank fraud pipeline.

Kafka (producer.py) and Spark (spark_consumer.py) are long-running
streaming processes and are intentionally NOT managed by this DAG —
they run continuously outside of Airflow's scheduling model.

This DAG owns the batch/transformation layer: on a schedule, it
rebuilds the dbt models (stg_transactions -> fraud_summary_daily)
on top of whatever rows Spark has written to raw_transactions so far,
then runs dbt tests to validate the output.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator

DBT_PROJECT_DIR = "/opt/airflow/dbt"
DBT_PROFILES_DIR = "/opt/airflow/dbt"

default_args = {
    "owner": "given",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


def check_postgres_connection():
    """
    Fails fast with a clear error if the warehouse Postgres isn't
    reachable, instead of letting a confusing dbt connection error
    surface three tasks deep.
    """
    hook = PostgresHook(postgres_conn_id="bank_pipeline_postgres")
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT 1;")
    result = cursor.fetchone()
    cursor.close()
    conn.close()
    if result != (1,):
        raise ValueError("Unexpected response from Postgres health check")


with DAG(
    dag_id="fraud_pipeline_dbt_orchestration",
    description="Periodically rebuilds dbt models on top of the streaming fraud data",
    default_args=default_args,
    schedule_interval=timedelta(minutes=15),
    start_date=datetime(2026, 7, 1),
    catchup=False,
    max_active_runs=1,
    tags=["bank-fraud-pipeline", "dbt", "phase6"],
) as dag:

    check_connection = PythonOperator(
        task_id="check_postgres_connection",
        python_callable=check_postgres_connection,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"dbt run "
            f"--project-dir {DBT_PROJECT_DIR} "
            f"--profiles-dir {DBT_PROFILES_DIR} "
            f"--target docker"
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"dbt test "
            f"--project-dir {DBT_PROJECT_DIR} "
            f"--profiles-dir {DBT_PROFILES_DIR} "
            f"--target docker"
        ),
    )

    check_connection >> dbt_run >> dbt_test
