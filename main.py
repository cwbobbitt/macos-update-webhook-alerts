import requests
import json
import urllib3

from datetime import datetime
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud import secretmanager

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
project_id = 
project_number = 
dataset_name = "macos_updates"
table_name = "updates"
secret_name = f'projects/{project_number}/secrets/service-account-json/versions/1'
webhook_url = 
query_full_table = "%s.%s.%s" % (project_id, dataset_name, table_name)
secret_client = secretmanager.SecretManagerServiceClient()
secret_request = secret_client.access_secret_version(name=secret_name)
service_info = json.loads(secret_request.payload.data.decode('UTF-8'))
creds = service_account.Credentials.from_service_account_info(service_info)
client = bigquery.Client(credentials=creds, project=project_id)
dataset_ref = client.dataset(dataset_name)
table_ref = dataset_ref.table(table_name)
table = client.get_table(table_ref) 
updates = []

url="https://gdmf.apple.com/v2/pmv"
get_updates = requests.get(url, verify=False)
macOS=json.loads(get_updates.content)['AssetSets']['macOS']
QUERY = (
    f""" SELECT * FROM `{project_id}.{dataset_name}.{table_name}`"""
    )
query_job = client.query(QUERY) 
rows = query_job.result() 
known_updates = rows.to_dataframe()

def send_chat(update, posting_date): 
    update_name = "macOS " + update
    release_date = datetime.strptime(posting_date, "%Y-%m-%d").strftime('%m/%d/%Y')
    data = {"cards": {
            'sections': [
                {
                'widgets': [
                    {
                    'keyValue': {
                        'topLabel': 'Title',
                        'content': update_name
                    }
                    },
                    {
                    'keyValue': {
                        'topLabel': 'Release Date',
                        'content': release_date
                    }
                    }
                ]
                }
            ],
            'header': {
                'title': 'Mac Update Detector',
                'subtitle': 'A new update has been released',
                'imageUrl': 'https://w7.pngwing.com/pngs/901/839/png-transparent-mac-mini-finder-macos-computer-icons-cool-miscellaneous-furniture-smiley.png',
                'imageStyle': 'IMAGE'
            }
            },
            "thread": {"threadKey": "macUpdates"}
            }
    requests.post(webhook_url, data=json.dumps(data), headers = {'Content-Type': 'application/json; charset=UTF-8'})

def update_bigquery(update):
    row_to_insert = [{'title': update}]
    client.insert_rows(table, row_to_insert)

for update in macOS:
    if update['ProductVersion'] not in known_updates['title'].values:
        #Add to bigquery array
        update_bigquery(update['ProductVersion'])
        #Make request to send chat webhook
        send_chat(update['ProductVersion'], update['PostingDate'])
        print("%s added to list of known updates and chat sent." % (update['ProductVersion']))
    else:
        print("%s is already a known update." % (update['ProductVersion']))
