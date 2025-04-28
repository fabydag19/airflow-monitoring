import pandas as pd
import re
import datetime
import pytz
import ssl
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS


# This functions is used to clean/prepare data for export
def clean_data(logger, data):
    try:
        logger.info('Data cleaning started...')

        # Calculate timestamp for delta_duration
        timestamp_now = int(datetime.datetime.now(pytz.utc).timestamp())

        # Set interested columns
        df = data[["dag_id", "dag_run_id", "state", "execution_date", "start_date", "end_date",]].copy()

        # For date columns:
        # 1. Delete microseconds
        # 2. Convert to datetime format
        # 3. Convert to epoch format
        columns = ["execution_date", "start_date", "end_date"]
        for col in columns:
            df[col] = df[col].apply(lambda x: re.sub(r"\..*(?=\+)", "", x) if pd.notnull(x) else "")
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.tz_convert(None)
            df[col] = df[col].astype(int) // 10**9

        # Calculate duration:
        # 1. If state is running, the duration is calculate from difference between now and start_date
        # 2. If state is success or failed, the duration is calculate from difference between end_date and start_date
        df["duration"] = df.apply(
            lambda x: (
                (timestamp_now - x["start_date"])
                if x["state"] == "running"
                else (
                    (x["end_date"] - x["start_date"])
                    if x["state"] in ["success", "failed"]
                    else 0
                )
            ),
            axis=1,
        )

        # For the start_date and end_date columns is necessary to convert it to a leggible format:
        # 1. Set to None if their value are negative
        # 2. Convert to datetime format YYYY-MM-DD hh:mm:ss plus GMT+02:00
        # 3. Convert to string, because InfluxDB support only _time as timestamp format and replace "NaT" string with null
        columns = ["start_date", "end_date"]
        for col in columns:
            df[col] = df[col].where(df[col] > 0, None)
            df[col] = pd.to_datetime(df[col], unit="s") + pd.Timedelta(hours=2)
            df[col] = df[col].astype(str).replace("NaT", "")

        logger.info('Data cleaning ended.')
        return df

    except Exception as e:
        logger.error(e)


# This functions is used to export data on InfluxDB
def influx_load_data(ROOT_DIR, config, logger, df):
    # Configuration of InfluxDB client
    token = config['influxdb']['token']
    org = config['influxdb']['org']
    bucket = config['influxdb']['bucket']
    url = config['influxdb']['url']

    # Create personalized context for SSL certificate check
    context = ssl.create_default_context(cafile=ROOT_DIR/'config'/'influxdb-cert.pem')

    try:
        logger.info('Data export to InfluxDB started...')

        # Set Influx client
        client = InfluxDBClient(url=url, token=token, org=org, ssl_context=context)
        write_api = client.write_api(write_options=SYNCHRONOUS)

        # Create a list of point for batch export
        points = []

        # Iterate on every row and column of dataframe
        for _, row in df.iterrows():
            point = (
                Point("dag_exec")
                .tag("dag_run_id", row["dag_run_id"])
                .tag("dag_id", row["dag_id"])
                .field("state", row["state"])
                .field("start_date", row["start_date"])
                .field("end_date", row["end_date"])
                .field("duration", row["duration"])
                .time(row["execution_date"], write_precision=WritePrecision.S)
            )
            points.append(point)
        
        # Write all points at once
        write_api.write(bucket=bucket, org=org, record=points)

        logger.info('Data export ended.')

    except Exception as e:
        logger.error(e)

    finally:
        client.close()
