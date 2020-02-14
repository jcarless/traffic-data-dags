import gzip
import logging
import os

from airflow.models import Variable
from google.cloud import storage


def load_incedent_details(**kwargs):

    logging.info("Loading data to GCS...")

    try:
        bucket_name = Variable.get("gcs_bucket")
        date = kwargs["ti"].xcom_pull(
            key="transformed_date", task_ids="transform_incedent_details")
        data = kwargs["ti"].xcom_pull(
            key="transformed_data", task_ids="transform_incedent_details")

        client = authenticate_client()
        key = f'traffic/incident_details/{date}.csv.gz'

        # Zip File
        zipped_file = gzip.compress(data.getvalue(), compresslevel=9)

        bucket = client.get_bucket(bucket_name)
        blob = bucket.blob(key)
        blob.upload_from_string(zipped_file)

        data.close()

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
