import airflow
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule

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
        "main_python_file_uri": "gs://python-scripts-etl/Crickbuzz_ranking_GCS.py",
    },
}

SPARK_JOB2 = {
    "reference": {"project_id": "dotted-banner-448417-n1"},
    "placement": {"cluster_name": "my-nyc-rest-cluster"},
    "pyspark_job": {
        "main_python_file_uri": "gs://python-scripts-etl/Crickbuzz_ranking_GCS.py",
    },
}

# Define the DAG
with DAG(
    dag_id='NYC_Resrurent_insp',
    default_args=default_args,
    description='A DAG to submit a PySpark job to GCP Dataproc',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2025, 1, 20),
    catchup=False,
    tags=['gcp', 'dataproc', 'spark'],
) as dag:

    start_cluster = BashOperator(
        task_id='start_cluster',
        bash_command="""
        gcloud dataproc clusters start my-nyc-rest-cluster \
        --region us-central1 \
        --project dotted-banner-448417-n1
        """
    )

    fetch_apidata = BashOperator(
    task_id='fetch_apidata',
    bash_command="""
        gsutil cp gs://python-scripts-etl/crickbuzz_ranking.py /tmp/crickbuzz_ranking.py
        python /tmp/crickbuzz_ranking.py
        """
    )

    
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
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    # Task to stop the Dataproc cluster
    stop_cluster = BashOperator(
        task_id='stop_cluster',
        bash_command="""
        gcloud dataproc clusters stop my-nyc-rest-cluster \
        --region us-central1 \
        --project dotted-banner-448417-n1
        """,
        trigger_rule=TriggerRule.NONE_FAILED
    )

    # The task is added to the DAG
    start_cluster >> load_to_GCS >> load_to_BQ >> stop_cluster
    load_to_GCS >> load_to_BQ >> stop_cluster  # If load_to_GCS fails, it proceeds to load_to_BQ and stop_cluster
    load_to_BQ >> stop_cluster  # If load_to_BQ fails, it proceeds to stop_cluster