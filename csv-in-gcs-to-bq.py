import datetime
from airflow import models
from airflow.contrib.operators import bigquery_operator
from airflow.contrib.operators import gcs_to_bq

#<<IMPORTANT>> Edit 2 global valuables with your appropreate bucket and dataset name!!
g_s_bucket = 'composer-handson-test'
g_d_dataset = 'composer_handson_admin_'
##--
g_d_table = 'd_table'

default_dag_args = {
    'start_date': datetime.datetime(2021,8,18),
}
with models.DAG(
    dag_id='csv-in-gcs-to-bq',
    schedule_interval=None,
    default_args=default_dag_args) as dag:

    # create dataset
    create_dataset = bigquery_operator.BigQueryCreateEmptyDatasetOperator(
        task_id='create_dataset',
        dataset_id=g_d_dataset,
    )

    # load csv data from gcs to bigquery
    load_csv = gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
        task_id='load_csv',
        bucket=g_s_bucket,
        source_objects=['babyname2017.csv'],
        destination_project_dataset_table=g_d_dataset+'.'+g_d_table,
        write_disposition='WRITE_TRUNCATE',
        schema_fields=[
          {'name':'name','type':'STRING','mode':'NULLABLE'},
          {'name':'sex','type':'STRING','mode':'NULLABLE'},
          {'name':'number','type':'INTEGER','mode':'NULLABLE'},
        ], 
    )

    create_dataset >> load_csv
