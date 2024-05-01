from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from helpers.test import multi
import datetime

def test_run():
    m = multi()
    print(m.multiplying(2,4))
    
    
default_args ={
    'owner': 'admin',
    'retries': 5,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='dag_testing',
    default_args=default_args,
    description = "testing dag",
    start_date = datetime.datetime(2024,4,29),
    schedule_interval = '@daily',   
) as dag:
    
    task1 = PythonOperator(
        task_id='test_run',
        python_callable=test_run
    )
    
task1