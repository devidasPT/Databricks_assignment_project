from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
}

with DAG(
    dag_id="superstore_sales_pipeline",
    default_args=default_args,
    schedule="@daily",   # ✅ NEW
    catchup=False,
) as dag:

    trigger_databricks_job = DatabricksRunNowOperator(
        task_id="superstore_sales_job",
        databricks_conn_id="databricks_default",
        job_id=306277206323200  # ← Replace with your job_id
    )
