def get_streamflow_data(url, station_id):
    # Send a GET request to the URL and store the response as a json object
    print(f"Getting streamflow data for station {station_id} from URL: {url}")
    response = requests.get(url)
    print(f"Response status code: {response.status_code}")
    response = response.json()

    # Extract the time series data from the json object
    time_series = None
    for ts in response['value']['timeSeries']:
        if ts['variable']['variableCode'][0]['value'] == '00060':
            time_series = ts['values'][0]['value']
            break

    # If time_series is still None, no data was found for variableCode '00060'
    if time_series is None:
        print(f"No data found for variableCode '00060' for station {station_id}, skipping.")
        return None

    # Create a list to store the data
    data = [{'datetime': ts['dateTime'], 'discharge': float(ts['value']), 'station_id': station_id} for ts in time_series]

    # Create a pandas dataframe from the data list
    df = pd.DataFrame(data)

    return df
