import logging
import requests


from airflow.hooks.base_hook import BaseHook


def extract_weather_forecast(lon, lat, **kwargs):
    logging.info("Extracting Weather Forecast...")

    URL = "https://api.darksky.net/forecast"

    try:
        api_key = BaseHook.get_connection("dark_sky").password
        url = f"{URL}/{api_key}/{lon},{lat}"
        r = requests.get(url=url)
        r.raise_for_status()
        data = r.json()

        kwargs["ti"].xcom_push(key="raw_data", value=data)

    except BaseException as e:
        logging.error("Failed to extract forecast from API!")
        raise e
