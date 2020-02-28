from airflow.hooks.mysql_hook import MySqlHook
from datetime import datetime
import pyspark.sql.functions as F
import io
import gzip
from google.cloud import storage
import os
import numpy as np
import pathlib


from pyspark.sql.types import (StructField, StringType, MapType, DoubleType,
                               IntegerType, StructType, TimestampType)
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
from airflow.exceptions import AirflowException
import logging
import requests
from pyspark.sql import SparkSession
from math import radians, cos, sin, asin, sqrt
from pyspark.sql import Window


def extract_incident_details(**kwargs):

    logging.info("Extracting incident details...")

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

        kwargs["ti"].xcom_push(key="raw_data", value=data)
        kwargs["ti"].xcom_push(key="raw_date", value=r.headers["Date"])

        return data, r.headers["Date"]

    except BaseException as e:
        raise AirflowException(
            "Failed to extract incident details from API: ", e)


if __name__ == "__main__":
    spark = SparkSession.builder.appName('traffic').getOrCreate()

    data, date = extract_incident_details()

    date_time = datetime.strptime(date, "%a, %d %b %Y %H:%M:%S %Z").replace(
        second=0, microsecond=0).isoformat()
    traffic_model_id = data["tm"]["@id"]

    temp_folder_path = "/usr/local/airflow/dags/operators/temp_incident_details"

    df = spark.createDataFrame(data["tm"]["poi"])
    df = df.select("cpoi")
    df = df.select(F.explode(df.cpoi))

    keys = (df
            .select(F.explode("col"))
            .select("key")
            .distinct()
            .rdd.flatMap(lambda x: x)
            .collect())

    exprs = [F.col("col").getItem(k).alias(k) for k in keys]
    df = df.select(*exprs)
    df = df.drop("cs")

    data_schema = StructType([
        StructField('length', StringType(), True),
        StructField('magnitude', StringType(), False),
        StructField('category', StringType(), False),
        StructField('from_street', StringType(), True),
        StructField('estimated_end', StringType(), True),
        StructField('location', StringType(), False),
        StructField('description', StringType(), True),
        StructField('cause', StringType(), True),
        StructField('road', StringType(), True),
        StructField('to_street', StringType(), True),
        StructField('delay', StringType(), True),
        StructField('incident_id', StringType(), False)
    ])

    df = spark.createDataFrame(df.rdd, schema=data_schema)

    df = df.withColumn("traffic_model_id", F.lit(traffic_model_id)) \
        .withColumn("length", df["length"].cast(IntegerType())) \
        .withColumn("estimated_end", df["estimated_end"].cast(TimestampType())) \
        .withColumn("delay", df["delay"].cast(IntegerType())) \
        .withColumn("date", F.lit(date_time).cast(TimestampType())) \
        .withColumn("category",
                    F.when(df["category"] == "0", "Unknown")
                    .when(df["category"] == "1", "Accident")
                    .when(df["category"] == "2", "Fog")
                    .when(df["category"] == "3", "Dangerous Conditions")
                    .when(df["category"] == "4", "Rain")
                    .when(df["category"] == "5", "Ice")
                    .when(df["category"] == "6", "Jam")
                    .when(df["category"] == "7", "Lane Closed")
                    .when(df["category"] == "8", "Road Closed")
                    .when(df["category"] == "9", "Road Works")
                    .when(df["category"] == "10", "Wind")
                    .when(df["category"] == "11", "Flooding")
                    .when(df["category"] == "12", "Detour")
                    ) \
        .withColumn("magnitude",
                    F.when(df["magnitude"] == "0", "Unknown")
                    .when(df["magnitude"] == "1", "Minor")
                    .when(df["magnitude"] == "2", "Moderate")
                    .when(df["magnitude"] == "3", "Major")
                    .when(df["magnitude"] == "4", "Undefined")
                    )

    df.createOrReplaceTempView('incidents')

    df = spark.sql(
        "SELECT *, split(location, '=|, y|}')[1] as lon, split(location, '=|, y|}')[3] as lat FROM incidents")

    df = df.withColumn("lon", df["lon"].cast(DoubleType())) \
        .withColumn("lat", df["lat"].cast(DoubleType())) \
        .drop("location")

    df = df.repartition(1)

    df.write.csv(
        path=temp_folder_path,
        mode="overwrite",
        header=True)

    # #####City ID######

    def get_temp_file_path():
        temp_file_path = None
        for file in os.listdir(temp_folder_path):
            if file.endswith(".csv"):
                temp_file_path = os.path.join(temp_folder_path, file)
        return temp_file_path

    schema = StructType([
        StructField('length', IntegerType(), True),
        StructField('magnitude', StringType(), False),
        StructField('category', StringType(), False),
        StructField('from_street', StringType(), True),
        StructField('estimated_end', TimestampType(), True),
        StructField('description', StringType(), True),
        StructField('cause', StringType(), True),
        StructField('road', StringType(), True),
        StructField('to_street', StringType(), True),
        StructField('delay', IntegerType(), True),
        StructField('incident_id', StringType(), False),
        StructField('traffic_model_id', StringType(), False),
        StructField('date', TimestampType(), False),
        StructField('lon', DoubleType(), False),
        StructField('lat', DoubleType(), False)
    ])

    df2 = spark.read.csv(path=get_temp_file_path(), header=True, schema=schema)
    print("TEMP FILE 1:", get_temp_file_path())

    # print("DF 2==========")
    # df2.show(5)
    # df2.printSchema()

    cities_schema = StructType([
        StructField('city_id', StringType(), False),
        StructField('city_name', StringType(), False),
        StructField('state', StringType(), True),
        StructField('city_lat', DoubleType(), False),
        StructField('city_lon', DoubleType(), False),
        StructField('location', StringType(), True),
    ])

    def get_cities():
        logging.info("Getting Cities Table...")
        mysql = MySqlHook(mysql_conn_id="cloud_sql_connection_mysql")
        connection = mysql.get_conn()

        with connection.cursor() as curs:
            try:
                query = f"""SELECT * FROM cities"""
                curs.execute(query)
            except BaseException as e:
                connection.rollback()
                raise AirflowException(f"""Query {query} failed: {e}""")
            else:
                city = curs.fetchall()

                return city

    cities_df = spark.createDataFrame(get_cities(), schema=cities_schema)

    def haversine(lon1, lat1, lon2, lat2):
        # convert decimal degrees to radians
        lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

        # haversine formula
        dlon = lon2 - lon1
        dlat = lat2 - lat1
        a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
        c = 2 * asin(sqrt(a))
        r = 3956  # Radius of earth in kilometers. Use 3956 for miles
        return c * r

    udf_haversine = F.udf(haversine)

    df2 = df2.crossJoin(cities_df)

    df2 = df2.withColumn("distance", udf_haversine(df2["lon"],
                                                   df2["lat"],
                                                   df2["city_lon"],
                                                   df2["city_lat"])
                         .cast(DoubleType()))

    w = Window.partitionBy('incident_id')

    df2 = df2.withColumn('min_dist', F.min(
        'distance').over(w).cast(DoubleType()))

    df2 = df2.where(df2["distance"] == df2["min_dist"])

    df2 = df2.drop("city_lat",
                   "city_lon",
                   "location",
                   "min_dist",
                   "city_name",
                   "state",
                   "distance")

    print("DF 2=========")
    df2.sort("distance", ascending=False).show(10)
    df2.printSchema()

    def load_incident_details(date):

        logging.info("Loading data to GCS...")

        try:
            bucket_name = "real-time-traffic"

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

        config = {
            "credentials_path": "/usr/local/airflow/dags/google_credentials.json"}
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = config['credentials_path']

        try:
            client = storage.Client()
        except BaseException as e:
            logging.error('Could not authenticate, {}'.format(e))
        else:
            logging.info('GCS authenticated')
            return client

    df2 = df2.repartition(1)

    print("===FINAL DF===")
    df2.show(5)

    print("PATH: ", temp_folder_path)

    df2.write.csv(
        path="/usr/local/airflow/dags/operators/temp",
        mode="overwrite",
        compression="gzip",
        header=True)

    # print("LOAD ALL: ", load_incident_details(temp_folder_path, date_time))
