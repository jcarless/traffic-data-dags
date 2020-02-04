from os.path import isfile, join
import logging
import requests
from datetime import datetime
import io
import os
import gzip
import json
import unicodecsv as csv
from google.cloud import storage

from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable


URL = f"https://api.tomtom.com/traffic/services"


def load_traffic_incident_details():

    api_key = BaseHook.get_connection("tomtom_api").password
    bounding_box = Variable.get("usa_bounding_box")
    bucket_name = Variable.get("gcs_bucket")

    with io.BytesIO() as csvfile:

        data_raw, date = get_incident_details(bounding_box, api_key)

        csvfile, date_transformed = transform_incident_details(csvfile, data_raw, date)

        load_to_gcs(csvfile, date_transformed, bucket_name)


def transform_incident_details(csvfile, data, date):
    traffic_model_id = data["tm"]["@id"]

    date_time = datetime.strptime(date, "%a, %d %b %Y %H:%M:%S %Z").replace(second=0, microsecond=0).isoformat()

    writer = csv.writer(csvfile, encoding="utf-8")
    writer.writerow(["traffic_model_id",
                     "incedent_id",
                     "date",
                     "location",
                     "category",
                     "magnitude",
                     "description",
                     "estimated_end",
                     "cause",
                     "from_street",
                     "to_street",
                     "length",
                     "delay",
                     "road"])

    for cluster in data["tm"]["poi"]:
        if "cpoi" in cluster:
            for incedent in cluster["cpoi"]:

                geo_json = {
                    "type": "Point",
                    "coordinates": [incedent["p"]["x"], incedent["p"]["y"]]
                }

                estimated_end = incedent["ed"] if "ed" in incedent else None

                writer.writerow([
                    traffic_model_id,
                    incedent["id"],
                    date_time,
                    geo_json,
                    category_switch(incedent["ic"]),
                    magnitude_switch(incedent["ty"]),
                    incedent["d"] if "d" in incedent else None,
                    estimated_end,
                    incedent["c"] if "c" in incedent else None,
                    incedent["f"] if "f" in incedent else None,
                    incedent["t"] if "t" in incedent else None,
                    incedent["l"] if "l" in incedent else None,
                    incedent["dl"] if "dl" in incedent else None,
                    incedent["r"] if "r" in incedent else None
                ])

    csvfile.seek(0)

    return csvfile, date_time


def load_to_gcs(csvfile, date_transformed, bucket_name):

    try:
        logging.info("Loading data to GCS...")

        # Zip File
        zipped_file = gzip.compress(csvfile.getvalue(), compresslevel=9)

        client = authenticate_client()
        key = f'traffic/incident_details/{date_transformed}.csv.gz'

        bucket = client.get_bucket(bucket_name)
        blob = bucket.blob(key)
        blob.upload_from_string(zipped_file)

    except BaseException as e:
        logging.error("Failed to load data to GCS!")
        raise e


def load_to_s3(data, date, bucket_name, s3_connection):
    logging.info("Uploading to s3...")

    try:
        s3 = S3Hook(aws_conn_id=s3_connection)

        key = f'traffic/incident_details/{date}.csv.gz'

        s3.load_bytes(data,
                      key=key,
                      bucket_name=bucket_name)

    except BaseException as e:
        logging.error("Failed to upload to s3!")
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


def load_to_bigquery(data, dataset_id, table_id, bigquery_creds):
    client = authenticate_client(bigquery_creds)

    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.skip_leading_rows = 1
    job_config.autodetect = False
    job = client.load_table_from_file(data, table_ref, job_config=job_config)
    job.result()  # Waits for table load to complete.

    logging.info("Loaded {} rows into {}:{}.".format(job.output_rows, dataset_id, table_id))


def get_incident_details(bounding_box, api_key):

    logging.info("Getting incedent details...")

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

        return data, r.headers["Date"]
    except BaseException as e:
        logging.error("Failed to extract incedent details from API!")
        raise e


def category_switch(argument):
    switcher = {
        0: "Unknown",
        1: "Accident",
        2: "Fog",
        3: "Dangerous Conditions",
        4: "Rain",
        5: "Ice",
        6: "Jam",
        7: "Lane Closed",
        8: "Road Closed",
        9: "Road Works",
        10: "Wind",
        11: "Flooding",
        12: "Detour"
    }
    return switcher.get(argument, "Invalid Category")


def magnitude_switch(argument):
    switcher = {
        0: "Unknown",
        1: "Minor",
        2: "Moderate",
        3: "Major",
        4: "Undefined"
    }
    return switcher.get(argument, "Invalid Magnitude")

# DISTANCE BETWEEN TWO POINTS
# import geopy.distance


# coords_1 = (52.2296756, 21.0122287)
# coords_2 = (52.406374, 16.9251681)
# print geopy.distance.vincenty(coords_1, coords_2).miles
if __name__ == "__main__":
    load_traffic_incident_details()
