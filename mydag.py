import json
import pathlib


import airflow.utils.dates
import requests
import requests.exceptions as requests_exceptions
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pendulum
import os
kst = pendulum.timezone("Asia/Seoul")


dag = DAG(
    dag_id = 'test_dag',
    description="practice dags",
    start_date=datetime(2024, 6,20, tzinfo=kst),
    schedule_interval="*/5 * * * *",
    catchup=False,
)


def _myfunc():
    with open("/home/hadoop/a.txt", "w") as f:
        f.write(f"Hello!!!! - {datetime.now()}")
   


write_time = PythonOperator(task_id='write_time',
                python_callable=_myfunc,
                dag=dag
              )




write_time
