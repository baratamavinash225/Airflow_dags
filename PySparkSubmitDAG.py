# airflowRedditPysparkDag.py
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
import os

sparkSubmit = '/usr/local/spark/bin/spark-submit'
srcDir = '/usr/local/spark'
redditFile = 'sample.csv'
## Define the DAG object
default_args = {
    'owner': 'airflo',
    'start_date': datetime(2019, 10, 15),
    'retries': 5,
    'retry_delay': timedelta(minutes=1),
}
dag = DAG('PysparkonAirflow', default_args=default_args, schedule_interval=timedelta(1))


#Submit PySpark job on the cluster
pySparkSubmit = BashOperator(
    task_id='unique-authors',
    bash_command=sparkSubmit + ' ' + srcDir + 'pyspark/helloWorldpyspark.py ' + redditFile,
    dag=dag)