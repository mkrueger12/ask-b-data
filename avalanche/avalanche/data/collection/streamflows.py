import pandas as pd
import requests

def get_streamflow_station_ids(state):
    # The URL for the USGS streamflow stations service in the specified state
    url = f'https://waterservices.usgs.gov/nwis/site/?format=rdb&stateCd={state}&siteType=ST&hasDataTypeCd=iv'

    # Send a request to the USGS API
    response = requests.get(url)

    # Parse the response into a list of station IDs
    station_ids = []
    for line in response.text.split('\n'):
        if line.startswith('USGS'):
            station_ids.append(line.split('\t')[1])

    # Return the list of station IDs
    return station_ids


url = 'https://waterservices.usgs.gov/nwis/dv/?format=json&sites=09066200&period=P365D&siteStatus=active'
# Read the data into a pandas DataFrame

# Send a GET request to the URL and store the response as a json object
response = requests.get(url).json()

# Extract the time series data from the json object
time_series = response['value']['timeSeries'][3]['values'][0]['value']

# Create a list to store the data
data = []

# Loop through the time series data and add it to the list
for i in range(len(time_series)):
    datetime = time_series[i]['dateTime']
    discharge = float(time_series[i]['value'])
    data.append({'datetime': datetime, 'discharge': discharge})

# Create a pandas dataframe from the data list
df = pd.DataFrame(data)