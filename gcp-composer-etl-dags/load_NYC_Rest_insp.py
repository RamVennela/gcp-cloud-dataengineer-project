from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

# Spark job configuration
SPARK_JOB1 = {
    "reference": {"project_id": "dotted-banner-448417-n1"},
    "placement": {"cluster_name": "my-nyc-rest-cluster"},
    "pyspark_job": {
        "main_python_file_uri": "gs://resturent_nyc_dataset/pyspark_scripts/nyc-rest-result_bronze.py",
    },
}

SPARK_JOB2 = {
    "reference": {"project_id": "dotted-banner-448417-n1"},
    "placement": {"cluster_name": "my-nyc-rest-cluster"},
    "pyspark_job": {
        "main_python_file_uri": "gs://resturent_nyc_dataset/pyspark_scripts/write_to_bigquery.py",
    },
}

# Define the DAG
with DAG(
    dag_id='submit_spark_job_to_dataproc',
    default_args=default_args,
    description='A DAG to submit a PySpark job to GCP Dataproc',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2025, 1, 25),
    catchup=False,
    tags=['gcp', 'dataproc', 'spark'],
) as dag:

    # Task to submit Spark job to Dataproc
    load_to_GCS = DataprocSubmitJobOperator(
        task_id='load_to_GCS',
        job=SPARK_JOB1,
        region="us-central1",
        project_id="dotted-banner-448417-n1",
        gcp_conn_id="google_cloud_default",
    )

    load_to_BQ = DataprocSubmitJobOperator(
        task_id='load_to_BQ',
        job=SPARK_JOB2,
        region="us-central1",
        project_id="dotted-banner-448417-n1",
        gcp_conn_id="google_cloud_default",
    )

    # The task is added to the DAG
    load_to_GCS >> load_to_BQ