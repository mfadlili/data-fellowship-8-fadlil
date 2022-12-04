import requests
import pandas as pd
import datetime as dt
from datetime import timedelta
from google.cloud import storage
import os

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator


PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
csv_filename = "us_population_yearly.csv"
parquet_filename = csv_filename.replace("csv", "parquet")
BIGQUERY_DATASET = "us_population"
            
def get_data(ti): 
    data = requests.get('https://datausa.io/api/data?drilldowns=Nation&measures=Population')
    nation = []
    year = []
    population = []

    for i in data.json()['data']:
        nation.append(i['Nation'])
        year.append(int(i['Year']))
        population.append(int(i['Population']))

    ti.xcom_push(key = 'nation', value = nation)
    ti.xcom_push(key = 'year', value = year)
    ti.xcom_push(key = 'population', value = population)

def to_csv(ti, csv_filename):
    nation = ti.xcom_pull(task_ids='get_data', key='nation')
    year = ti.xcom_pull(task_ids='get_data', key='year')
    population = ti.xcom_pull(task_ids='get_data', key='population')
    df = pd.DataFrame({'nation':nation, 'year':year, 'population':population})
    df.to_csv('/opt/airflow/dags/'+csv_filename)

def to_parquet(csv_filename, parquet_filename):
    df = pd.read_csv('/opt/airflow/dags/'+csv_filename)
    df.to_parquet('/opt/airflow/dags/'+parquet_filename)

def upload_to_gcs(project_id, bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client(project_id)
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)

def delete_file(csv_filename, parquet_filename):
    os.remove(f"/opt/airflow/dags/{csv_filename}")
    os.remove(f"/opt/airflow/dags/{parquet_filename}")

default_args = {
    'owner': 'Fadlil',
    'start_date': dt.datetime(2022, 10, 26),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5),
}

with DAG('dag1',
         default_args=default_args,
         schedule_interval=timedelta(minutes=10),      # '0 * * * *',
         ) as dag:

    
    task1 = PythonOperator(task_id='get_data',
                                 python_callable=get_data)

    task2 = PythonOperator(
        task_id='save_to_csv',
        python_callable=to_csv,
        op_kwargs = {"csv_filename": csv_filename})

    task3 = PythonOperator(
        task_id='save_to_parquet',
        python_callable=to_parquet,
        op_kwargs = {"csv_filename": csv_filename, "parquet_filename":parquet_filename})
    
    task4 = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "project_id" : PROJECT_ID,
            "bucket": BUCKET,
            "object_name": f"raw/{parquet_filename}",
            "local_file": f"/opt/airflow/dags/{parquet_filename}",
        },
    )

    task5 = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": "external_table",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/raw/{parquet_filename}"],
            },
        },
    )

    task6 = PythonOperator(
        task_id='delete_local_file',
        python_callable=delete_file,
        op_kwargs = {"csv_filename": csv_filename, "parquet_filename":parquet_filename})

task1 >> task2 >> task3 >> task4 >> task5 >> task6