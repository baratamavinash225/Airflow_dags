from airflow import DAG
from datetime import timedelta, datetime
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable
from airflow.operators import HiveOperator


import re
import os
from datetime import datetime, timedelta


DAG_DEFAULT_ARGS = {
        'owner': 'airflow',
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=1)
}

with DAG('HIVE_SUBMIT_TEST', description='Hive SUBMIT DAG', schedule_interval='@daily', start_date=datetime(2018, 11, 1), default_args=DAG_DEFAULT_ARGS, catchup=False) as dag:

user_defined_macros = {}

local_macros = {
      'local_hive_settings': """
          SET mapred.job.queue.name=default;
     """,
}

user_defined_macros.update(local_macros)

hql_file_path = "/home/airflow/hql/run_hiveQuery.hql"
print(hql_file_path)
run_hive_query = HiveOperator(
      task_id='run_hive_query',
      dag = dag,
      hql = """
      {{ local_hive_settings }}
      """ + "\n " + open(hql_file_path, 'r').read()
)
