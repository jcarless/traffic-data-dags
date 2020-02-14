"""
Code that goes along with the Airflow located at:
http://airflow.readthedocs.org/en/latest/tutorial.html
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils import dates

from operators.extract_incedent_details import extract_incedent_details
from operators.load_incedent_details import load_incedent_details
from operators.transform_incedent_details import transform_incedent_details

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime.utcnow() - timedelta(minutes=6),
    "email": "carless.jerome@gmail.com",
    "email_on_failure": False,
    "email_on_retry": False,
    "provide_context": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG("traffic_incedent_details",
         default_args=default_args,
         catchup=False,
         schedule_interval='*/5 * * * *') as dag:

    # Extract
    extract_incedent_details = PythonOperator(
        task_id="extract_incedent_details",
        python_callable=extract_incedent_details,
    )
    # Transform
    transform_incedent_details = PythonOperator(
        task_id="transform_incedent_details",
        python_callable=transform_incedent_details,
    )

    # Load
    load_incedent_details = PythonOperator(
        task_id="load_incedent_details",
        python_callable=load_incedent_details,
    )

    extract_incedent_details >> transform_incedent_details >> load_incedent_details
