import pandas as pd
import requests


def get_streamflow_station_ids(state):
    # The URL for the USGS streamflow stations service in the specified state
    url = f'https://waterservices.usgs.gov/nwis/site/?format=rdb&stateCd={state}&siteType=ST&hasDataTypeCd=iv'

    # Send a request to the USGS API
    response = requests.get(url)

    # Parse the response into a list of station IDs
    station_ids = [line.split('\t')[1] for line in response.text.split('\n') if line.startswith('USGS')]

    # Return the list of station IDs
    return station_ids


def get_streamflow_data(url, station_id):
    # Send a GET request to the URL and store the response as a json object
    response = requests.get(url)
    print(response.status_code)
    response = response.json()
    print(url)

    # Extract the time series data from the json object
    time_series = None
    for ts in response['value']['timeSeries']:
        if ts['variable']['variableCode'][0]['value'] == '00060':
            time_series = ts['values'][0]['value']
            break

    # If time_series is still None, no data was found for variableCode '00060'
    if time_series is None:
        print(f"No data found for variableCode '00060' for station_id: {station_id}")
        return pd.DataFrame()

    # Create a list to store the data
    data = [{'datetime': ts['dateTime'], 'discharge': float(ts['value']), 'station_id': station_id} for ts in
            time_series]

    # Create a pandas dataframe from the data list
    df = pd.DataFrame(data)

    return df


ids = get_streamflow_station_ids('CO')

df = []

for i in ids:
    data = get_streamflow_data(
        f'https://waterservices.usgs.gov/nwis/dv/?format=json&sites={i}&period=P365D&siteStatus=active', i)
    if not data.empty:
        df.append(data)

# dfs = [get_streamflow_data(f'https://waterservices.usgs.gov/nwis/dv/?format=json&sites={i}&period=P365D&siteStatus=active') for i in ids]

# df = pd.concat(dfs)
