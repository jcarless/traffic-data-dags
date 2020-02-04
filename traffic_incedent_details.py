"""
Code that goes along with the Airflow located at:
http://airflow.readthedocs.org/en/latest/tutorial.html
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.utils import dates

from operators.us_traffic_incident_details import load_traffic_incident_details

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime.utcnow() - timedelta(minutes=3),
    "email": "carless.jerome@gmail.com",
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}

dag = DAG("load_traffic_incident_details", default_args=default_args, catchup=False, schedule_interval='*/3 * * * *')

load_traffic_incident_details = PythonOperator(
    task_id="load_traffic_incident_details",
    python_callable=load_traffic_incident_details,
    dag=dag,
)

[load_traffic_incident_details]
