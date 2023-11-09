import asyncio
import logging
from typing import Any, Dict, Union, List, Tuple

import google.cloud.logging
from io import StringIO

import numpy as np
import pandas as pd
import requests
from google.cloud import bigquery
from google.cloud import storage
from pandas import Series, DataFrame

'''gcloud functions deploy snotel-data \
  --gen2 \
  --runtime=python310 \
  --trigger-topic=Daily-Pull \
  --set-env-vars TZ="America/Denver" \
  --entry-point=entry_point \
  --min-instances=0 \
  --max-instances=5 \
  --memory=512MB \
  --timeout=540s \
  --region=us-central1 \
  --ingress-settings=internal-only \
  --no-allow-unauthenticated
'''


##### CONFIG #####
# Specify the dataset and table information
project_id = 'avalanche-analytics-project'
dataset_id = 'production'
table_id = 'snotel'

client = bigquery.Client(project=project_id)
log_client = google.cloud.logging.Client()
log_client.setup_logging()


def upload_blob_from_memory(bucket: storage.Bucket, contents: str, destination_blob_name: str) -> None:
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_string(contents, content_type='text/csv')
    logging.info(f"{destination_blob_name} uploaded to {bucket.name}.")


def process_station(record: Dict[str, Any]) -> Union[None, Tuple[str, Any, Any]]:
    today_data = None
    yesterday_data = None

    station_id = record["site_name"][record["site_name"].find("(") + 1:record["site_name"].find(")")]
    state = record["state"]

    logging.info(f"Processing station {station_id} in {state}")

    url = f'https://wcc.sc.egov.usda.gov/reportGenerator/view_csv/customSingleStationReport/daily/start_of_period/{station_id}:{state}:SNTL%7Cid=""|name/-1,0/name,stationId,state.code,network.code,elevation,latitude,longitude,county.name,WTEQ::value,WTEQ::pctOfMedian_1991,SNWD::value,TMAX::value,TMIN::value,TOBS::value,SNDN::value?fitToScreen=false'

    response = requests.get(url)

    # Check if the request was successful
    if response.status_code == 200:
        # Use StringIO to simulate a file object for Pandas to read the CSV data
        csv_data = StringIO(response.text)

        # Parse CSV data into a Pandas DataFrame
        data = pd.read_csv(csv_data, comment='#', skip_blank_lines=True)

        if 'Snow Depth (in) Start of Day Values' not in data.columns:
            data['Snow Depth (in) Start of Day Values'] = None

        data['new_snow'] = np.maximum(0, data['Snow Depth (in) Start of Day Values'] - data[
                'Snow Depth (in) Start of Day Values'].shift(1))

        column_mapping = {
                'Date': 'date',
                'Station Name': 'station_name',
                'Station Id': 'station_id',
                'State Code': 'state_code',
                'Network Code': 'network_code',
                'Elevation (ft)': 'elevation_ft',
                'Latitude': 'latitude',
                'Longitude': 'longitude',
                'County Name': 'county_name',
                'Snow Water Equivalent (in) Start of Day Values': 'snow_water_equivalent_in',
                'Snow Water Equivalent % of Median (1991-2020)': 'snow_water_equivalent_median_percentage',
                'Snow Depth (in) Start of Day Values': 'snow_depth_in',
                'Air Temperature Maximum (degF)': 'max_temp_degF',
                'Air Temperature Minimum (degF)': 'min_temp_degF',
                'Air Temperature Observed (degF) Start of Day Values': 'observed_temp_degF',
                'Snow Density (pct) Start of Day Values': 'snow_density_percentage'
            }

        data.rename(columns=column_mapping, inplace=True)

        data = data.fillna(np.nan).replace([np.nan], [None])

        schema = {
            "date": "datetime64[ns]",
            "station_name": "string",
            "station_id": "Int64",
            "state_code": "string",
            "network_code": "string",
            "elevation_ft": "Int64",
            "latitude": "float64",
            "longitude": "float64",
            "county_name": "string",
            "snow_water_equivalent_in": "float64",
            "snow_water_equivalent_median_percentage": "float64",
            "snow_depth_in": "float64",
            "max_temp_degF": "float64",
            "min_temp_degF": "float64",
            "observed_temp_degF": "float64",
            "snow_density_percentage": "float64",
            "new_snow": "float64"
        }

        # Ensure columns match the schema
        for column, dtype in schema.items():
            if column not in data.columns or data[column].dtype != dtype:
                data[column] = data[column].astype(dtype)

        if len(data) == 0:
            return station_id, today_data, yesterday_data

        if len(data[1:]) > 0:
            today_data = data[1:]

        if len(data[0:1]) > 0:
            yesterday_data = data[0:1]

        logging.info(f"Successfully processed station {station_id} in {state}")

        return station_id, today_data, yesterday_data

    else:
        print("Failed to fetch data from the URL")

        logging.warning(f"Failed to fetch data for  station {station_id} in {state}, Url: {url}")

        return None


async def append_bq_table(df: pd.DataFrame, _table_id: str) -> None:

    logging.info(f"Appending {len(df)} rows to {project_id}.{dataset_id}.{_table_id}")

    table_ref = client.dataset(dataset_id).table(_table_id)

    # Load data into the table
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND

    # Load data from the DataFrame into the BigQuery table
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()  # Wait for the job to complete

    logging.info(f"STATUS: {job.state}")


async def update_bq_table(df: pd.DataFrame) -> None:

    logging.info(f"Updating {project_id}.{dataset_id}.{table_id}")

    # Define the update SQL statement
    update_sql = f"""
        UPDATE `{project_id}.{dataset_id}.{table_id}`
        SET 
            snow_water_equivalent_in = @snow_water_equivalent_in,
            snow_water_equivalent_median_percentage = @snow_water_equivalent_median_percentage,
            snow_depth_in = @snow_depth_in,
            max_temp_degF = @max_temp_degF,
            min_temp_degF = @min_temp_degF,
            observed_temp_degF = @observed_temp_degF,
            snow_density_percentage = @snow_density_percentage
        WHERE station_id = @station_id AND date = @date
    """

    update_row = df.to_dict(orient='records')[0]

    # Define the parameters for the SQL statement
    parameters = [
        bigquery.ScalarQueryParameter("station_id", "INT64", update_row['station_id']),
        bigquery.ScalarQueryParameter("date", "DATE", update_row['date'].strftime('%Y-%m-%d')),
        bigquery.ScalarQueryParameter("snow_water_equivalent_in", "FLOAT", update_row['snow_water_equivalent_in']),
        bigquery.ScalarQueryParameter("snow_water_equivalent_median_percentage", "FLOAT",
                                      update_row['snow_water_equivalent_median_percentage']),
        bigquery.ScalarQueryParameter("snow_depth_in", "FLOAT", update_row['snow_depth_in']),
        bigquery.ScalarQueryParameter("max_temp_degF", "FLOAT", update_row['max_temp_degF']),
        bigquery.ScalarQueryParameter("min_temp_degF", "FLOAT", update_row['min_temp_degF']),
        bigquery.ScalarQueryParameter("observed_temp_degF", "FLOAT", update_row['observed_temp_degF']),
        bigquery.ScalarQueryParameter("snow_density_percentage", "FLOAT", update_row['snow_density_percentage'])
    ]

    # Run the update query
    update_sql = update_sql.format(project_id=project_id, dataset_id=dataset_id, table_id=table_id)
    query_job = client.query(update_sql, job_config=bigquery.QueryJobConfig(query_parameters=parameters))
    query_job.result()  # Wait for the query to complete

    logging.info(f"STATUS: {query_job.state}")



def entry_point(event: Any, context: Any) -> None:
    # Initialize Google Cloud Storage client and bucket
    storage_client = storage.Client(project=project_id)
    bucket = storage_client.bucket('snow-depth')

    # Fetch station metadata
    station_md = pd.read_html("https://wcc.sc.egov.usda.gov/nwcc/yearcount?network=sntl&state=&counttype=statelist")[0].to_dict(orient='records')

    for record in station_md:

        try:

            station_id, today_data, yesterday_data = process_station(record)

            if today_data is not None:

                destination_blob_name = f'daily_raw/{str(today_data["date"][1])}-{str(today_data["station_id"][1])}.csv'
                upload_blob_from_memory(bucket, contents=today_data.to_csv(index=False),
                                        destination_blob_name=destination_blob_name)

                asyncio.run(append_bq_table(today_data, table_id))

            if yesterday_data is not None:

                asyncio.run(update_bq_table(yesterday_data))

        except TypeError as e:
            logging.error(f"Error occurred: {str(e)}")

