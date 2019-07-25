from airflow import DAG
from datetime import timedelta, datetime
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable
import sys 

DAG_DEFAULT_ARGS = {
'owner': 'airflow',
'depends_on_past': False,
'retries': 1,
'retry_delay': timedelta(minutes=1)
}

with DAG('SPARK_SUBMIT_TEST', description='SPARK SUBMIT DAG', schedule_interval='@daily', start_date=datetime(2018, 11, 1), default_args=DAG_DEFAULT_ARGS, catchup=False) as dag:

spark_submit_operator = SparkSubmitOperator(
                master="local",
      task_id="spark_submit_job",
      java_class="org.apache.spark.examples.SparkPi",
      application_args=["10","--with-spaces", "args should keep embedded spaces"],
      application="/opt/spark/examples/jars/spark-examples_2.11-2.3.2.jar",
      dag=dag)
	  