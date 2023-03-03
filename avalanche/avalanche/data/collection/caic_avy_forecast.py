import pandas as pd
import requests
from datetime import datetime, timezone, timedelta
from google.cloud import storage



def build_avy_df(url):

    data = requests.get(url)
    data = data.json()

    df_final = []

    for i in range(0, len(data)):
        print(i)

        record = data[i]

        if record['type'] != 'avalancheforecast':
            print("PASS")
            pass
        else:
            print('YAY')
            probs = pd.json_normalize(record['avalancheProblems']['days'][0]).rename({'type': 'problem'}, axis='columns')
            conf = pd.json_normalize(record['confidence']['days'][0]).rename({'rating': 'confidence'}, axis='columns').drop(['date'], axis=1)
            danger = pd.json_normalize(record['dangerRatings']['days']).drop(['date'], axis=1)
            danger = danger[danger['position'] == 1]


            df = {'id': record['id'],
                  'title': record['title'],
                  'type': record['type'],
                  'areaId': record['areaId'],
                  'forecaster': record['forecaster'],
                  'issueDate': record['issueDateTime'],
                  'expiryDate': record['expiryDateTime']
                  }

            df = pd.DataFrame(df, index=[0])
            df = pd.concat([df, probs, conf, danger], axis=1).ffill()
            df = df.drop(['type'], axis=1)

            df_final.append(df)

    df_final = pd.concat(df_final)

    return df_final


def upload_blob_from_memory(bucket_name, contents, destination_blob_name):
    """Uploads a file to the bucket."""

    # The ID of your GCS bucket
    # bucket_name = "your-bucket-name"

    # The contents to upload to the file
    # contents = "these are my contents"

    # The ID of your GCS object
    # destination_blob_name = "storage-object-name"
    #service_account_info = json.load(open('/home/max/Documents/Access Accounts/avalanche-analytics-project-55735591e108.json'))
    #storage_client = storage.Client(credentials = service_account.Credentials.from_service_account_info(service_account_info))
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_string(contents.to_csv(), 'text/csv')

    print(
        f"{destination_blob_name} uploaded to {bucket_name}."
    )


def main(event, context):
    now = str(datetime.now(timezone.utc))

    now = f'{now[0:10]}T{now[11:16]}:00.000Z'

    url = f'https://avalanche.state.co.us/api-proxy/avid?_api_proxy_uri=/products/all?datetime={now}&includeExpired=true'
    print(url)

    #url = 'https://avalanche.state.co.us/api-proxy/avid?_api_proxy_uri=/products/all&includeExpired=true'

    data = build_avy_df(url)

    today = datetime.today()

    yesterday = str(today - timedelta(days=1))[0:10]

    upload_blob_from_memory("raw-avy-data", data, f'daily/av-forecast/{yesterday}.csv')


