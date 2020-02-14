import io
import logging
from datetime import datetime

import unicodecsv as csv
from airflow.exceptions import AirflowException


def transform_incedent_details(**kwargs):

    logging.info("Transforming Incedent Details...")

    try:
        date = kwargs["ti"].xcom_pull(
            key="raw_date", task_ids="extract_incedent_details")
        data = kwargs["ti"].xcom_pull(
            key="raw_data", task_ids="extract_incedent_details")

        traffic_model_id = data["tm"]["@id"]

        date_time = datetime.strptime(date, "%a, %d %b %Y %H:%M:%S %Z").replace(
            second=0, microsecond=0).isoformat()

        csvfile = io.BytesIO()

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

        kwargs["ti"].xcom_push(key="transformed_data", value=csvfile)
        kwargs["ti"].xcom_push(key="transformed_date", value=date_time)

    except BaseException as e:
        raise AirflowException(
            "Failed to transform incedent details: ", e)


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
