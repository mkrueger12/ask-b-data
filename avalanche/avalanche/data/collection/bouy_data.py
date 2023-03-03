import requests
from geopy.distance import geodesic
import pandas as pd
from datetime import datetime
from google.cloud import storage

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


def main(event, context):
    # Define the coordinates of Hawaii and Juneau
    hawaii_lat, hawaii_lon = 19.8968, -155.5828
    juneau_lat, juneau_lon = 58.3019, -134.4197
    radius_juneau = 700
    radius_hawaii = 1000

    # Make a request to the NBDC API to get the buoy data
    nbdc_url = "https://www.ndbc.noaa.gov/data/latest_obs/latest_obs.txt"
    response = requests.get(nbdc_url)

    # Split the response into lines and extract the relevant columns
    lines = response.text.splitlines()
    buoy_data = []
    for line in lines[2:]:
        cols = line.split()
        buoy_id = cols[0]
        lat = float(cols[1])
        lon = float(cols[2])
        buoy_data.append({"buoy_id": buoy_id, "latitude": lat, "longitude": lon})

    # Create an empty list to store the buoy IDs within 500 miles of Hawaii or Juneau
    buoys_within_500_miles = []

    # Loop through each buoy location and calculate the distance to Hawaii and Juneau
    for buoy in buoy_data:
        buoy_lat, buoy_lon = buoy["latitude"], buoy["longitude"]
        buoy_coords = (buoy_lat, buoy_lon)
        distance_to_hawaii = geodesic((hawaii_lat, hawaii_lon), buoy_coords).miles
        distance_to_juneau = geodesic((juneau_lat, juneau_lon), buoy_coords).miles
        if distance_to_hawaii <= radius_hawaii or distance_to_juneau <= radius_juneau:
            buoys_within_500_miles.append(buoy["buoy_id"])

    df_header = pd.read_csv(nbdc_url, nrows=1, header=None)
    df_units = pd.read_csv(nbdc_url, skiprows=1, nrows=1, header=None)

    # Combine the header rows into a single DataFrame
    df_header = pd.concat([df_header, df_units], axis=1)
    header_list = df_header.iloc[0].str.split().tolist()

    # Read the remaining data from the API into a pandas DataFrame
    df_data = pd.read_csv(nbdc_url, delim_whitespace=True, skiprows=range(0,2))

    # Set the header of the DataFrame to the combined header column
    df_data.columns = pd.MultiIndex.from_arrays(header_list)

    # Rename the first column to "station_id" using the column index
    df_data = df_data.rename(columns={df_data.columns[0]: "station_id"})

    # Combine the levels of the column index
    df_data.columns = [f"{a}_{b}" if b else a for a, b in df_data.columns]
    df_data.rename(columns={'#STN_#text': "station_id"}, inplace=True)

    df_data = df_data[df_data['station_id'].isin(buoys_within_500_miles)]

    upload_blob_from_memory("raw-avy-data", df_data, f'daily/bouy/bouy_{datetime.now()}.csv')
