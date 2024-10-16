import requests
import json
import os
import pandas as pd
from requests.auth import HTTPBasicAuth
#from airflow.models import Variable

API_KEY = '4ad88535ed5db0323b8a8e4cbacd9ab8'
base_url = 'https://api.openweathermap.org/data/2.5/weather?q='
data_dir = './raw_files/'

headers = {'Accept': 'application/json'}
auth = HTTPBasicAuth('apikey', API_KEY)
import datetime

def get_weather():
    #cities = Variable.get(key="cities")
    cities = ['paris', 'london', 'washington']
    responses = []
    for city in cities:
        url = base_url + city + "&appid=" + API_KEY
        req = requests.get(url, headers=headers)
        response = req.json()
        responses.append(response)
    
    time = datetime.datetime.now()
    filename = data_dir + time.strftime("%Y-%m-%d %H:%M") +".json"
    
    with open(filename, "w") as f:
        json.dump(responses,f)

def transform_data_into_csv(n_files=None, filename='data.csv'):
    parent_folder = './raw_files'
    files = sorted(os.listdir(parent_folder), reverse=True)
    if n_files:
        files = files[:n_files]

    dfs = []

    for f in files:
        with open(os.path.join(parent_folder, f), 'r') as file:
            data_temp = json.load(file)
        print(data_temp)
        for data_city in data_temp:
            dfs.append(
                {
                    'temperature': data_city['main']['temp'],
                    'city': data_city['name'],
                    'pression': data_city['main']['pressure'],
                    'date': f.split('.')[0]
                }
            )

    df = pd.DataFrame(dfs)

    print('\n', df.head(10))

    df.to_csv(os.path.join('./clean_data', filename), index=False)
 
   
if __name__ == '__main__':
    #get_weather()
    print(os.getcwd())
    transform_data_into_csv(5)
    transform_data_into_csv(filename="fulldata.csv")
    