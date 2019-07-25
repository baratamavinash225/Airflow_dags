from airflow import DAG
from datetime import timedelta, datetime
from airflow.contrib.operators.spark_submit_operator import 
SparkSubmitOperator
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable

dag = DAG('SPARK_SUBMIT_TEST',start_date=datetime(2018,12,10), 
schedule_interval='0 12 * * *')


_config ={'application':'hadoop@10.70.1.35:/home/hadoop/pyspark_script/pi.py',
    'master' : 'local',
    'deploy-mode' : 'client',
    'executor_cores': 1,
    'EXECUTORS_MEM': '2G'
}

spark_submit_operator = SparkSubmitOperator(
    task_id='spark_submit_job',
    dag=dag,
    **_config)