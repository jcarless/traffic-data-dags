"""
Code that goes along with the Airflow located at:
http://airflow.readthedocs.org/en/latest/tutorial.html
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.utils import dates

from operators.extract_weather_forecast import extract_weather_forecast
from operators.load_weather_forecast import load_weather_forecast
from operators.transform_weather_forecast import transform_weather_forecast

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime.utcnow() - timedelta(minutes=70),
    "email": "carless.jerome@gmail.com",
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    'provide_context': True,
    "catchup": False,
}

with DAG("weather_forecast",
         default_args=default_args,
         schedule_interval="@hourly") as dag:

    postgres = PostgresHook(postgres_conn_id="cloud_sql_connection")
    connection = postgres.get_conn()

    with connection.cursor() as curs:
        try:
            query = "SELECT city_name, lon, lat FROM cities"
            curs.execute(query)
        except BaseException as e:
            connection.rollback()
            raise AirflowException(f"""Query {query} failed: {e}""")
        else:
            cities = curs.fetchall()

            for city, lon, lat in cities:
                city = city.replace(" ", "_")

                # Extract
                extract_weather_forecast_operator = PythonOperator(
                    task_id=f"extract_{city}_forecast",
                    python_callable=extract_weather_forecast,
                    op_kwargs={"lon": 40.7127837, "lat": -74.0059413}
                )

                # Transform
                transform_weather_forecast_operator = PythonOperator(
                    task_id=f"transform_{city}_forecast",
                    python_callable=transform_weather_forecast,
                    op_kwargs={
                        "upstream_task_id": extract_weather_forecast_operator.task_id
                    }
                )

                # Load
                load_weather_forecast_operator = PythonOperator(
                    task_id=f"load_{city}_forecast",
                    python_callable=load_weather_forecast,
                    op_kwargs={
                        "city": city,
                        "upstream_task_id": transform_weather_forecast_operator.task_id
                    }
                )

                extract_weather_forecast_operator >> transform_weather_forecast_operator >> load_weather_forecast_operator
