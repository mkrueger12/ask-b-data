import requests
import pandas as pd

# define the API endpoint
url = 'https://www.ndbc.noaa.gov/radial_search.php'

# set the coordinates for Hawaii
lat = 21.3069
lon = -157.8583

# set the search radius in nautical miles
radius = 500

# make the API request with the search parameters
response = requests.get(url, params={
    'lat': lat,
    'lon': lon,
    'radius': radius,
})

# extract the buoy IDs from the API response
buoy_ids = []
for line in response.text.splitlines():
    if 'station_page.php?station=' in line:
        buoy_id = line.split('station=')[1].split('"')[0]
        buoy_ids.append(buoy_id)

# define the API endpoint to get the buoy data
api_endpoint = 'https://www.ndbc.noaa.gov/data/realtime2/{}/{}.txt'

# define a dictionary to store the buoy data
buoy_data = {}

# loop through the buoy IDs and get the buoy data
for buoy_id in buoy_ids:
    try:
        # make the API request to get the buoy data
        response = requests.get(api_endpoint.format(buoy_id, buoy_id[-2:]))

        # parse the buoy data into a pandas DataFrame
        data = pd.read_csv(
            pd.compat.StringIO(response.text),
            delim_whitespace=True,
            comment='#',
            header=None,
            names=['year', 'month', 'day', 'hour', 'minute', 'wind_dir', 'wind_spd', 'gust', 'wave_height',
                   'dominant_wave_period', 'average_wave_period', 'wave_dir', 'sea_temp', 'atmospheric_pressure',
                   'pressure_tendency'],
            index_col=pd.to_datetime({'year': pd.Series([], dtype='int'), 'month': pd.Series([], dtype='int'),
                                      'day': pd.Series([], dtype='int'), 'hour': pd.Series([], dtype='int'),
                                      'minute': pd.Series([], dtype='int')}),
            parse_dates=True,

