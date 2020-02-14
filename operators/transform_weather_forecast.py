import logging
import json


from airflow.exceptions import AirflowException


def transform_weather_forecast(upstream_task_id, **kwargs):
    logging.info("Transforming Weather Forecast...")

    try:
        forecast = kwargs["ti"].xcom_pull(
            key="raw_data", task_ids=upstream_task_id)

        print("FORECAST: ", forecast)

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

        kwargs["ti"].xcom_push(key="transformed_data", value=current_weather)

    except BaseException as e:
        raise AirflowException(
            "Failed to transform weather forecast: ", e)


def create_jsonlines(original):

    if isinstance(original, str):
        original = json.loads(original)

    return '\n'.join([json.dumps(original[outer_key], sort_keys=True)
                      for outer_key in sorted(original.keys(),
                                              key=lambda x: int(x))])
