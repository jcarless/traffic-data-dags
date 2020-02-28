"""
Code that goes along with the Airflow located at:
http://airflow.readthedocs.org/en/latest/tutorial.html
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils import dates

from operators \
    .traffic_incident_details_ops \
    .extract_incident_details import extract_incident_details

from operators \
    .traffic_incident_details_ops \
    .load_incident_details import load_incident_details

from operators \
    .traffic_incident_details_ops \
    .transform_incident_details import transform_incident_details

from operators \
    .traffic_incident_details_ops \
    .geo_locate_cities import geo_locate_cities

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime.utcnow() - timedelta(minutes=6),
    "email": "carless.jerome@gmail.com",
    "email_on_failure": False,
    "email_on_retry": False,
    "provide_context": True,
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}

with DAG("traffic_incident_details",
         default_args=default_args,
         catchup=False,
         schedule_interval=None) as dag:

    # Extract
    extract_incident_details = PythonOperator(
        task_id="extract_incident_details",
        python_callable=extract_incident_details
    )

    # Transform
    transform_incident_details = PythonOperator(
        task_id="transform_incident_details",
        python_callable=transform_incident_details
    )

    # Geolocate Cities
    geo_locate_cities = PythonOperator(
        task_id="geo_locate_cities",
        python_callable=geo_locate_cities
    )

    # Load
    load_incident_details = PythonOperator(
        task_id="load_incident_details",
        python_callable=load_incident_details
    )

    (extract_incident_details
     >> transform_incident_details
     >> geo_locate_cities
     >> load_incident_details)
