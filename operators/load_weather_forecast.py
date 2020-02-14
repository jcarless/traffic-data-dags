import gzip
import io
import json
import logging
import os
import datetime


from airflow.exceptions import AirflowException
from airflow.models import Variable
from google.cloud import storage


def load_weather_forecast(city, upstream_task_id, ** kwargs):

    try:
        logging.info("Loading data to GCS...")

        bucket_name = Variable.get("gcs_bucket")
        data = kwargs["ti"].xcom_pull(
            key="transformed_data", task_ids=upstream_task_id)

        zipped_data = zip_json(data)
        client = authenticate_client()
        date_time = datetime.datetime.fromtimestamp(
            data["ts"]).replace(second=0, microsecond=0).isoformat()
        key = f'traffic/weather/{city}/current/{date_time}.json.gz'

        bucket = client.get_bucket(bucket_name)
        blob = bucket.blob(key)
        blob.upload_from_string(zipped_data)

    except BaseException as e:
        raise AirflowException(
            "Failed to load weather forecast: ", e)


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
