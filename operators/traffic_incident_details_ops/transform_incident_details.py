import io
import logging
from datetime import datetime

from airflow.exceptions import AirflowException
from airflow.hooks.mysql_hook import MySqlHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, lit, udf, when
from pyspark.sql.types import (DoubleType, IntegerType, StringType,
                               StructField, StructType, TimestampType)


def transform_incident_details(**kwargs):
    logging.info("Transforming incident Details WITH SPARK...")

    try:
        # Get variables and data from upstream tasks
        date = kwargs["ti"] \
            .xcom_pull(key="raw_date",
                       task_ids="extract_incident_details")

        data = kwargs["ti"] \
            .xcom_pull(key="raw_data",
                       task_ids="extract_incident_details")

        date_time = datetime \
            .strptime(date, "%a, %d %b %Y %H:%M:%S %Z") \
            .replace(second=0, microsecond=0) \
            .isoformat()

        # Set the path to save dataframe to
        temp_folder_path = "usr/local/airflow/dags/temp/temp_transform_incident_details"

        # Creat Spark Session
        spark = SparkSession.builder.appName('traffic').getOrCreate()

        # Create the dataframe
        df = create_df(spark, data, date_time)

        # Parse coordinates and create lon, lat columns
        df = parse_coordinates(spark, df)

        # Save dataframe
        save_df(df, temp_folder_path)

        kwargs["ti"].xcom_push(key="transformed_date", value=date_time)
        kwargs["ti"].xcom_push(key="temp_folder_path", value=temp_folder_path)

    except BaseException as e:
        raise AirflowException(
            "Failed to transform incident details WITH SPARK: ", e)


def parse_coordinates(spark, df):

    try:
        logging.info("Parsing Coordinates")

        # Create a temp view for sql query
        df.createOrReplaceTempView('incidents')

        # Parse x, y coordinates and create lon, lat columns
        df = spark.sql(
            "SELECT *, split(location, '=|, y|}')[1] as lon, split(location, '=|, y|}')[3] as lat FROM incidents")

        df = df.withColumn("lon", df["lon"].cast(DoubleType())) \
            .withColumn("lat", df["lat"].cast(DoubleType())) \
            .drop("location")

        return df

    except BaseException as e:
        raise AirflowException(
            "Failed to parse coordinates: ", e)


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
            header=True)

    except BaseException as e:
        raise AirflowException(
            "Failed to save dataframe: ", e)


def create_df(spark, data, date_time):
    try:
        logging.info("Creating dataframe...")

        traffic_model_id = data["tm"]["@id"]

        # Create DataFrame
        df = spark.createDataFrame(data["tm"]["poi"])

        # Filter down to relevant data
        df = df.select("cpoi")
        df = df.select(explode(df.cpoi))

        # Get the keys of the values in cpoi field
        keys = (df
                .select(explode("col"))
                .select("key")
                .distinct()
                .rdd.flatMap(lambda x: x)
                .collect())

        # Conver to columns
        exprs = [col("col").getItem(k).alias(k) for k in keys]
        df = df.select(*exprs)

        # Removed unneeded cs column
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

        # Recreate dataframe with schema
        df = spark.createDataFrame(df.rdd, schema=data_schema)

        # Add missing columns and transform data types
        df = df.withColumn("traffic_model_id", lit(traffic_model_id)) \
            .withColumn("length", df["length"].cast(IntegerType())) \
            .withColumn("estimated_end", df["estimated_end"].cast(TimestampType())) \
            .withColumn("delay", df["delay"].cast(IntegerType())) \
            .withColumn("date", lit(date_time).cast(TimestampType())) \
            .withColumn("category",
                        when(df["category"] == "0", "Unknown")
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
                        when(df["magnitude"] == "0", "Unknown")
                        .when(df["magnitude"] == "1", "Minor")
                        .when(df["magnitude"] == "2", "Moderate")
                        .when(df["magnitude"] == "3", "Major")
                        .when(df["magnitude"] == "4", "Undefined")
                        )

        return df

    except BaseException as e:
        raise AirflowException(
            "Failed to create the dataframe: ", e)


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
