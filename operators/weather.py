import requests
import pytz
import datetime
import io
import os
import csv
import json
import gzip
import logging

from google.cloud import storage

from airflow.hooks.base_hook import BaseHook
from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable


logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')

URL = "https://api.darksky.net/forecast"


def load_forecast(lon, lat, city, **kwargs):

    api_key = BaseHook.get_connection("dark_sky").password
    bucket_name = Variable.get("gcs_bucket")

    forecast = get_forecast(lon, lat, api_key)
    forecast_transformed = transform_forecast(forecast)

    if hasattr(forecast, 'alerts'):
        alerts_transformed = transform_alerts(forecast)

        load_to_gcs(type="alerts",
                    city=city,
                    data=alerts_transformed,
                    bucket_name=bucket_name)

    load_to_gcs(type="current",
                city=city,
                data=forecast_transformed,
                bucket_name=bucket_name)


def load_to_s3(type, city, data, bucket_name, s3_connection, kwargs):
    try:
        print("Loading data to s3...")
        zipped_data = zip_json(data)
        s3 = S3Hook(aws_conn_id=s3_connection)
        date_time = datetime.datetime.fromtimestamp(
            data["ts"]).replace(second=0, microsecond=0).isoformat()
        key = f'traffic/weather/{city}/{type}/{date_time}.json.gz'

        s3.load_bytes(zipped_data,
                      key=key,
                      bucket_name=bucket_name)

    except BaseException as e:
        print("Failed to load data to s3!")
        raise e


def load_to_gcs(type, city, data, bucket_name):

    try:
        logging.info("Loading data to GCS...")

        zipped_data = zip_json(data)
        client = authenticate_client()
        date_time = datetime.datetime.fromtimestamp(
            data["ts"]).replace(second=0, microsecond=0).isoformat()
        key = f'traffic/weather/{city}/{type}/{date_time}.json.gz'

        bucket = client.get_bucket(bucket_name)
        blob = bucket.blob(key)
        blob.upload_from_string(zipped_data)

    except BaseException as e:
        logging.error("Failed to load data to GCS!")
        raise e


def authenticate_client():
    """
    returns an authenticated client
    :return:
        a bigquery.Client()
    """

    logging.info('Authenticating GCS...')

    config = Variable.get("bigquery", deserialize_json=True)
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = config['credentials_path']

    try:
        client = storage.Client()
    except BaseException as e:
        logging.error('Could not authenticate, {}'.format(e))
    else:
        logging.info('GCS authenticated')
        return client


def get_forecast(lon, lat, api_key):
    logging.info("Getting weather forecast...")

    try:
        url = f"{URL}/{api_key}/{lon},{lat}"
        r = requests.get(url=url)
        r.raise_for_status()
        data = r.json()

        return data
    except BaseException as e:
        logging.error("Failed to extract forecast from API!")
        raise e


def transform_forecast(forecast):
    logging.info("Transforming Current Weather...")

    currently = forecast["currently"]

    location = {
        "0": {"type": "Point", "coordinates": [
            forecast["latitude"], forecast["longitude"]]}
    }

    location = create_jsonlines(location)

    current_weather = {
        "ts": currently["time"],
        "timezone": forecast["timezone"],
        "location": location,
        "summary": currently["summary"],
        "nearest_storm_distance": currently["nearestStormDistance"],
        "visibility": currently["visibility"],
        "temp": currently["temperature"],
        "apparent_temp": currently["apparentTemperature"],
        "wind_speed": currently["windSpeed"],
        "wind_gust": currently["windGust"],
        "uv_index": currently["uvIndex"],
        "cloud_cover": currently["cloudCover"],
        "precip_type": currently["precipType"] if hasattr(currently, 'precipType') else None,
        "precip_prob": currently["precipProbability"],
        "precip_intensity": currently["precipIntensity"],
        "precip_intensity_error": currently["precipIntensityError"] if hasattr(currently, 'precipIntensityError') else None
    }

    return current_weather


def transform_alerts(forecast):
    logging.info("Transforming Alerts...")

    alerts = forecast["alerts"]

    location = {"0": {"type": "Point", "coordinates": [
        alerts["latitude"], alerts["longitude"]]}}
    location = create_jsonlines(location)

    alerts_transformed = {
        "location": location,
        "ts": alerts["time"],
        "expires": datetime.datetime.fromtimestamp(alerts["expires"]),
        "title": alerts["title"],
        "description": alerts["description"],
        "uri": alerts["uri"]
    }

    return alerts_transformed


def zip_json(data):
    logging.info("Zipping data...")
    try:
        gz_body = io.BytesIO()
        gz = gzip.GzipFile(None, 'wb', 9, gz_body)
        gz.write(json.dumps(data).encode('utf-8'))
        gz.close()
        return gz_body.getvalue()

    except BaseException as e:
        logging.error("Zip failed!")
        raise e


def create_jsonlines(original):

    if isinstance(original, str):
        original = json.loads(original)

    return '\n'.join([json.dumps(original[outer_key], sort_keys=True)
                      for outer_key in sorted(original.keys(),
                                              key=lambda x: int(x))])


if __name__ == "__main__":
    load_forecast(40.7127837, -74.0059413, "New_York")
