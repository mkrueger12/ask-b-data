import datetime
from io import StringIO

import numpy as np
import pandas as pd
import requests
from google.cloud import storage


def upload_blob_from_memory(bucket, contents, destination_blob_name):
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_string(contents, content_type='text/csv')
    print(f"{destination_blob_name} uploaded to {bucket.name}.")


def process_station(record):
    station_id = record["site_name"][record["site_name"].find("(") + 1:record["site_name"].find(")")]
    state = record["state"]

    url = f'https://wcc.sc.egov.usda.gov/reportGenerator/view_csv/customSingleStationReport/daily/start_of_period/{station_id}:{state}:SNTL%7Cid=""|name/0,0/name,stationId,state.code,network.code,elevation,latitude,longitude,county.name,WTEQ::value,WTEQ::pctOfMedian_1991,SNWD::value,TMAX::value,TMIN::value,TOBS::value,SNDN::value?fitToScreen=false'
    #https://wcc.sc.egov.usda.gov/reportGenerator/view/customSingleStationReport/daily/start_of_period/1120:CO:SNTL%7Cid=%22%22%7Cname/-29,0/WTEQ::value,WTEQ::pctOfMedian_1991,SNWD::value,TMAX::value,TMIN::value,TOBS::value,SNDN::value?fitToScreen=false
    print('Done with', station_id, 'in', state, 'at', datetime.datetime.now().strftime("%H:%M:%S"))

    response = requests.get(url)

    # Check if the request was successful
    if response.status_code == 200:
        # Use StringIO to simulate a file object for Pandas to read the CSV data
        csv_data = StringIO(response.text)

        # Parse CSV data into a Pandas DataFrame
        data = pd.read_csv(csv_data, comment='#', skip_blank_lines=True)

        if data['Snow Depth (in) Start of Day Values'] not in data.columns:
            data['Snow Depth (in) Start of Day Values'] = np.nan

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

        return station_id, data[1:]

    else:
        print("Failed to fetch data from the URL")

        return None





def entry_point(request):
    # Initialize Google Cloud Storage client and bucket
    storage_client = storage.Client(project='avalanche-analytics-project')
    bucket = storage_client.bucket('snow-depth')

    # Fetch station metadata
    station_md = pd.read_html("https://wcc.sc.egov.usda.gov/nwcc/yearcount?network=sntl&state=&counttype=statelist")[
        0].to_dict(orient='records')

    processed_data = []
    for record in station_md:

        try:

            station_id, data = process_station(record)

            print(station_id)
            processed_data.append((station_id, data))

            # Upload data to Google Cloud Storage in bulk
            for station_id, data in processed_data:
                destination_blob_name = f'daily_raw/{data["date"]}_{station_id}.csv'
                upload_blob_from_memory(bucket, contents=data.to_csv(index=False),
                                        destination_blob_name=destination_blob_name)

        except TypeError:
            pass


