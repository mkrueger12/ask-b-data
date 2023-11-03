import pandas as pd
import datetime
from google.cloud import storage
from google.oauth2 import service_account
import json
import os



def upload_blob_from_memory(bucket_name, contents, destination_blob_name):
    """Uploads a file to the bucket."""

    # The ID of your GCS bucket
    # bucket_name = "your-bucket-name"

    # The contents to upload to the file
    # contents = "these are my contents"

    # The ID of your GCS object
    # destination_blob_name = "storage-object-name"
    storage_client = storage.Client(project='avalanche-analytics-project')
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_string(contents, 'text/csv')

    print(
        f"{destination_blob_name} uploaded to {bucket_name}."
    )


#Station Metadata

station_md = pd.read_html("https://wcc.sc.egov.usda.gov/nwcc/yearcount?network=sntl&state=&counttype=statelist")[0].to_dict(orient='records')
station_md = [entry for entry in station_md if entry['state'] == 'CO']


for i in range(0,len(station_md)):
    record = station_md[i]

    s = str()

    station_id = record["site_name"][record["site_name"].find("(")+1:record["site_name"].find(")")]

    state = record["state"]

    start_date = record["start"]

    # format
    format = '%Y-%B'

    # convert from string format to datetime format
    s_date = datetime.datetime.strptime(start_date, format)

    s_date = str(s_date.date())

    # build url

    url = f'https://wcc.sc.egov.usda.gov/reportGenerator/view/customSingleStationReport/daily/start_of_period/{station_id}:{state}:SNTL%7Cid=%22%22%7Cname/{s_date},2023-02-28/stationId,name,SNWD::value,SNWD::qcFlag,SNWD::qaFlag,SNWD::prevValue?fitToScreen=false'

    print(url)

    # read in snowtel daata

    data = max(pd.read_html(url), key=lambda x: len(x))

    data['state'] = state

    upload_blob_from_memory('snow-depth', contents=data.to_csv(), destination_blob_name='raw/{station_id}.csv')
