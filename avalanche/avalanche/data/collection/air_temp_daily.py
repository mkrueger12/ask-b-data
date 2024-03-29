import os

import pandas as pd
import datetime
from google.cloud import storage
from google.oauth2 import service_account

def upload_blob_from_memory(bucket_name, contents, destination_blob_name):
    """Uploads a file to the bucket."""

    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)

        blob.upload_from_string(contents.to_csv(), 'text/csv')

        print(f"{destination_blob_name} uploaded to {bucket_name}.")
    except Exception as e:
        print(f"Error uploading file to {bucket_name}: {e}")


def snotel_swe_daily(station_data):
    record = station_data

    station_id = record["site_name"][record["site_name"].find("(") + 1:record["site_name"].find(")")]

    state = record["state"]

    # build url


    # Get today's date
    today = datetime.date.today()


    # Yesterday date
    yesterday = today - datetime.timedelta(days=1)

    url = f'https://wcc.sc.egov.usda.gov/reportGenerator/view/customSingleStationReport/daily/start_of_period/{station_id}:{state}:SNTL%7Cid=%22%22%7Cname/{yesterday},{yesterday}/stationId,name,TOBS::value,TMIN::value,TMAX::value,TAVG::value,TOBS::qcFlag,TOBS::qaFlag?fitToScreen=false'

    print(url)

    # read in snowtel daata

    data = pd.read_html(url)

    # snow_depth
    snow_data = data[34]

    if len(snow_data.axes[1]) > 10:
        snow_data = 1

    return snow_data, station_id



def main(event, context):
    #Station Metadata


    station_md = pd.read_html("https://wcc.sc.egov.usda.gov/nwcc/yearcount?network=sntl&state=&counttype=statelist")[1]

    station_md = station_md[station_md['state'] == 'CO']

    # Get today's date
    today = datetime.date.today()


    # Yesterday date
    yesterday = today - datetime.timedelta(days=1)

    for i in range(0, len(station_md)):
        record = station_md.iloc[i]

        data, station_id = snotel_swe_daily(record)

        if data == 1:
            print('NO RECORDS', station_id)
            pass
        else:

            upload_blob_from_memory("raw-avy-data", data, f'daily/air-temp/daily_temp_{station_id}_{yesterday}.csv')

    print('SUCCESS')

