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
    service_account_info = json.load(open('/home/max/Documents/Access Accounts/avalanche-analytics-project-55735591e108.json'))
    storage_client = storage.Client(credentials = service_account.Credentials.from_service_account_info(service_account_info))
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_string(snow_data.to_csv(), 'text/csv')

    print(
        f"{destination_blob_name} uploaded to {bucket_name}."
    )


# Read all files in directory


file = os.listdir('/home/max/Documents/Avalanche')
file = [i.split('_',)[3] for i in file]
file = [i.replace(".csv", "") for i in file]

#Station Metadata


station_md = pd.read_html("https://wcc.sc.egov.usda.gov/nwcc/yearcount?network=sntl&state=&counttype=statelist")[1]


all = []


for i in range(0,len(station_md)):
    record = station_md.iloc[i]

    s = str()

    station_id = record["site_name"][record["site_name"].find("(")+1:record["site_name"].find(")")]

    if station_id in file:
        print('pass', station_id)
        pass
    else:

        state = record["state"]

        start_date = record["start"]

        # datetime in string format for may 25 1999
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

        data = pd.read_html(url)

        #Station ID

        station = data[5]

        #snow_depth
        snow_data = data[34]

        #all.append(snow_data)

        #data = pd.concat(all)

        if len(snow_data) > 2:

            snow_data.to_csv(f'/home/max/Documents/Avalanche/historical_snow_depth_{station_id}.csv')

        else:
            pass

