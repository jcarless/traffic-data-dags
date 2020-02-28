import logging
import os
from math import asin, cos, radians, sin, sqrt

from airflow.exceptions import AirflowException
from airflow.hooks.mysql_hook import MySqlHook

import pyspark.sql.functions as F
from pyspark.sql import SparkSession, Window
from pyspark.sql.types import (DoubleType, IntegerType, MapType, StringType,
                               StructField, StructType, TimestampType)


def geo_locate_cities(**kwargs):
    try:
        # Create spark session
        spark = SparkSession.builder.appName('traffic').getOrCreate()

        # Get the temp folder path
        temp_folder_path = kwargs["ti"].xcom_pull(
            key="temp_folder_path", task_ids="transform_incident_details")

        # Get the temp file path
        temp_file_path = get_temp_file_path(temp_folder_path)

        # Get the temp dataframe
        incidents_df = get_spark_df(spark, temp_file_path)

        # Get cities dataframe
        cities_df = get_cities_df(spark)

        # Cross join the incidents_df and cities_df
        df_with_city_id = assign_city_id(incidents_df, cities_df)

        # Set path to save dataframe to
        new_temp_folder_path = "/usr/local/airflow/dags/temp/temp_geo_locate_cities"

        # Save dataframe
        save_df(df_with_city_id, new_temp_folder_path)

        kwargs["ti"].xcom_push(key="temp_folder_path",
                               value=new_temp_folder_path)

    except BaseException as e:
        raise AirflowException(
            "geo_locate_cities failed: ", e)


def save_df(df, temp_folder_path):
    # Repartition to a single partition so the dataframe
    # will be saved to a single file.
    # This file is not expected to change in size too much
    # so this is worth it for the convenience.

    try:
        logging.info(f"Saving Dataframe to {temp_folder_path}")
        df = df.repartition(1)

        df.write.csv(
            path=temp_folder_path,
            mode="overwrite",
            compression="gzip",
            header=True)

    except BaseException as e:
        raise AirflowException(
            "Failed to save dataframe: ", e)


def assign_city_id(incidents_df, cities_df):

    try:

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

        logging.info("Assigning city_id to incidents...")
        # Register haversine function as a spark UDF
        udf_haversine = F.udf(haversine)

        # Cross join incidents_df with cities_df
        # This will create a row for each incident <> city pair
        joined_df = incidents_df.crossJoin(cities_df)

        # Calculate the distance between incidents and cities coordinates
        # Add result to new distance column
        joined_df = joined_df \
            .withColumn("distance", udf_haversine(joined_df["lon"],
                                                  joined_df["lat"],
                                                  joined_df["city_lon"],
                                                  joined_df["city_lat"])
                        .cast(DoubleType()))

        # Partition the df by incident_id.
        # For each partition (aka incident_id) find the minimum distance value.
        # Set this value to the new min_dist column for every row in the partition.
        w = Window.partitionBy('incident_id')

        joined_df = joined_df.withColumn('min_dist', F.min(
            'distance').over(w).cast(DoubleType()))

        # Keep only the rows that represent
        # the incident <> city pair with the minimum distance
        joined_df = joined_df.where(
            joined_df["distance"] == joined_df["min_dist"])

        # Drop unneeded columns
        joined_df = joined_df.drop("city_lat",
                                   "city_lon",
                                   "location",
                                   "min_dist",
                                   "city_name",
                                   "state",
                                   "distance")
        return joined_df

    except BaseException as e:
        raise AirflowException(
            "Failed to assign city_id: ", e)


def get_cities_df(spark):
    try:
        # Query the cities table from db.
        cities = get_cities()

        logging.info("Creating cities dataframe...")

        cities_schema = StructType([
            StructField('city_id', StringType(), False),
            StructField('city_name', StringType(), False),
            StructField('state', StringType(), True),
            StructField('city_lat', DoubleType(), False),
            StructField('city_lon', DoubleType(), False),
            StructField('location', StringType(), True),
        ])

        return spark.createDataFrame(cities, schema=cities_schema)

    except BaseException as e:
        raise AirflowException(
            "Failed to create cities dataframe: ", e)


def get_spark_df(spark, temp_file_path):
    try:
        logging.info("Creating Incidents dataframe...")

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

        df = spark.read.csv(path=temp_file_path, header=True, schema=schema)

        return df

    except BaseException as e:
        raise AirflowException(
            "Failed to get dataframe: ", e)


def get_temp_file_path(temp_folder_path):

    try:
        logging.info("Getting temporary file path...")
        temp_file_path = None

        for file in os.listdir(temp_folder_path):
            if file.endswith(".csv"):
                temp_file_path = os.path.join(temp_folder_path, file)
        return temp_file_path

    except BaseException as e:
        raise AirflowException(
            "Failed to get temporary file path: ", e)


def get_cities():
    try:
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

    except BaseException as e:
        raise AirflowException(
            "Failed to query cities table from the database: ", e)
