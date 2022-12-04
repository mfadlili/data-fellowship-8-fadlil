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
    id_nation = []
    nation = []
    id_year = []
    year = []
    population = []
    slug_nation = []

    for i in data.json()['data']:
        id_nation.append(i['ID Nation'])
        nation.append(i['Nation'])
        id_year.append(int(i['ID Year']))
        year.append(int(i['Year']))
        population.append(int(i['Population']))
        slug_nation.append(i["Slug Nation"])

    result = {'ID Nation':id_nation, 'Nation':nation, "ID Year":id_year, "Year":year, "Population":population, "Slug Nation":slug_nation}
    ti.xcom_push(key = 'parsing_result', value = result)

def to_csv(ti, csv_filename):
    dict_result = ti.xcom_pull(task_ids='get_data', key='parsing_result')
    df = pd.DataFrame(dict_result)
    df.to_csv('/opt/airflow/dags/'+csv_filename, index=False)

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
                "tableId": "us_pop_yearly",
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