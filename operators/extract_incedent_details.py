from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
from airflow.exceptions import AirflowException
import logging
import requests


def extract_incedent_details(**kwargs):

    logging.info("Extracting incedent details...")

    # Make this an airflow variable
    URL = f"https://api.tomtom.com/traffic/services"

    api_key = BaseHook.get_connection("tomtom_api").password
    bounding_box = Variable.get("usa_bounding_box")

    try:
        PARAMS = {
            'projection': 'EPSG4326',
            'key': api_key,
            'expandCluster': 'true'
        }

        style = "s3"
        zoom = 7
        format = "json"
        versionNumber = 4
        url = f"{URL}/4/incidentDetails/{style}/{bounding_box}/{zoom}/-1/{format}"

        r = requests.get(url=url, params=PARAMS)
        r.raise_for_status()
        data = r.json()

        print("CONTEXT: ", kwargs)

        kwargs["ti"].xcom_push(key="raw_data", value=data)
        kwargs["ti"].xcom_push(key="raw_date", value=r.headers["Date"])

    except BaseException as e:
        raise AirflowException(
            "Failed to extract incedent details from API: ", e)


if __name__ == "__main__":
    extract_incedent_details()
