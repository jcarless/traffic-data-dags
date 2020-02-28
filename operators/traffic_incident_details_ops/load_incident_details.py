import gzip
import logging
import os

from airflow.models import Variable
from google.cloud import storage


def load_incident_details(**kwargs):

    logging.info("Loading data to GCS...")

    try:
        bucket_name = Variable.get("gcs_bucket")

        temp_folder_path = kwargs["ti"].xcom_pull(
            key="temp_folder_path", task_ids="geo_locate_cities")

        date = kwargs["ti"].xcom_pull(
            key="transformed_date", task_ids="transform_incident_details")

        client = authenticate_client()
        bucket = client.get_bucket(bucket_name)
        files = os.listdir(temp_folder_path)

        for file in files:
            if file.endswith(".csv.gz"):
                key = f'traffic/incident_details/{date}/{file}'
                blob = bucket.blob(key)
                blob.content_encoding = 'gzip'
                blob.upload_from_filename(
                    os.path.join(temp_folder_path, file))
                logging.info(f"{file} loaded")

    except BaseException as e:
        logging.error("Failed to load data to GCS!")
        raise e


def authenticate_client():
    """
    returns an authenticated client
    :return:
        a storage.Client()
    """

    logging.info('authenticating with GCS...')

    config = Variable.get("bigquery", deserialize_json=True)
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = config['credentials_path']

    try:
        client = storage.Client()
    except BaseException as e:
        logging.error('Could not authenticate, {}'.format(e))
    else:
        logging.info('GCS authenticated')
        return client
