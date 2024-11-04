from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
import logging 

import datetime
import requests
import json
import os
import pandas as pd
from joblib import dump 

from airflow.models import Variable
import datetime
from airflow.operators.python import BranchPythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import DummyOperator

from sklearn.model_selection import cross_val_score
from sklearn.linear_model import LinearRegression
from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import RandomForestRegressor

API_KEY = ''
base_url = 'https://api.openweathermap.org/data/2.5/weather?q='
data_dir = '/app/raw_files/'

headers = {'Accept': 'application/json'}

def setup():
    Variable.set('cities',"[\"paris\", \"london\", \"washington\"]")

def get_weather():
    cities = Variable.get(key='cities',deserialize_json=True)

    if cities == None:
        raise Exception('Json deserialize error')
    
    responses = []
    for i in range(3):
        url = base_url + cities[i] + "&appid=" + API_KEY
        req = requests.get(url, headers=headers)
        response = req.json()
        responses.append(response)
    
    time = datetime.datetime.now()
    filename = data_dir + time.strftime("%Y-%m-%d %H:%M") +".json"
    with open(filename, "w") as f:
        json.dump(responses,f)

def transform_data_into_csv(n_files=None, filename='data.csv'):
    parent_folder = '/app/raw_files'
    files = sorted(os.listdir(parent_folder), reverse=True)
    if n_files:
        files = files[:n_files]

    dfs = []
    
    for f in files:
        with open(os.path.join(parent_folder, f), 'r') as file:
            data_temp = json.load(file)
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

    df.to_csv(os.path.join('/app/clean_data', filename), index=False)

def compute_model_score(model, X, y):
    # computing cross val
    cross_validation = cross_val_score(
        model,
        X,
        y,
        cv=3,
        scoring='neg_mean_squared_error')

    model_score = cross_validation.mean()
    
    return model_score


def train_and_save_model(model_id,path_to_model='./model.pckl'):
    X, y = prepare_data('/app/clean_data/fulldata.csv')
    #choose model
    if model_id == 1:
        model = LinearRegression()
    elif model_id == 2: 
        model = DecisionTreeRegressor()
    else:
        model = RandomForestRegressor()
    # training the model      
    model.fit(X, y)
    # saving model
    print(str(model), 'saved at ', path_to_model)
    dump(model, path_to_model)


def prepare_data(path_to_data='/app/clean_data/fulldata.csv'):
    # reading data
    df = pd.read_csv(path_to_data)
    
    # ordering data according to city and date
    df = df.sort_values(['city', 'date'], ascending=True)

    dfs = []

    for c in df['city'].unique():
        df_temp = df[df['city'] == c]

        print(df_temp['temperature'].shift(1)
)
        # creating target
        df_temp.loc[:, 'target'] = df_temp['temperature'].shift(1)

        # creating features
        for i in range(1, 10):
            df_temp.loc[:, 'temp_m-{}'.format(i)
                        ] = df_temp['temperature'].shift(-i)

        # deleting null values
        print("df temp:",df_temp)
        df_temp = df_temp.dropna()
        
        dfs.append(df_temp)

    # concatenating datasets
    df_final = pd.concat(
        dfs,
        axis=0,
        ignore_index=False
    )

    # deleting date variable
    df_final = df_final.drop(['date'], axis=1)

    # creating dummies for city variable
    df_final = pd.get_dummies(df_final)

    features = df_final.drop(['target'], axis=1)
    target = df_final['target']

    return features, target

def training(model, task_instance):
    X, y = prepare_data('/app/clean_data/fulldata.csv')
    score = compute_model_score(model, X, y)
    task_instance.xcom_push(
        key="score",
        value=score
    )

    
def choose_model(task_instance):
    # 1 - modèle linéaire
    # 2 - modèle decision tree
    # 3 - random forest
    tolerance = 1e-9
    scores = task_instance.xcom_pull(
            key="score",
            task_ids=['group_4.task_41','group_4.task_42','group_4.task_43']
    )
    
    logging.info(scores)
    if len(scores) == 0:
        return
    
    if float(scores[0]) - float(scores[1]) > tolerance:
        if float(scores[0]) - float(scores[2]) > tolerance:
            train_and_save_model(1)
        else:
            train_and_save_model(3)
    elif float(scores[1]) - float(scores[2]) > tolerance:
        train_and_save_model(2)
    else:
        train_and_save_model(3)
            
            
    
with DAG(
    dag_id='weather_extract_data',
    tags=['extract','weather'],
    schedule_interval=None,
    default_args={
        'owner': 'airflow',
        'start_date': days_ago(0, 1)
    },
    catchup=False
) as my_dag:

    task0 = PythonOperator(
        task_id='task0',
        python_callable=setup
    )
    
    task1 = PythonOperator(
        task_id='task1',
        python_callable=get_weather
    )
    
    task2 = PythonOperator(
        task_id='task2',
        python_callable=transform_data_into_csv,
        op_kwargs={'n_files': 20}
    )
    
    task3 = PythonOperator(
        task_id='task3',
        python_callable=transform_data_into_csv,
        op_kwargs={'filename': 'fulldata.csv'}
    )
    
    with TaskGroup("group_4") as group_4:
        task_41 = PythonOperator(
            task_id='task_41',
            python_callable=training,
            op_kwargs={'model': LinearRegression()},
            trigger_rule="all_success"
        )
        task_42 = PythonOperator(
            task_id='task_42',
            python_callable=training,
            op_kwargs={'model': DecisionTreeRegressor()},
            trigger_rule="all_success"
        )
        task_43 = PythonOperator(
            task_id='task_43',
            python_callable=training,
            op_kwargs={'model': RandomForestRegressor()},
            trigger_rule="all_success"
        )
        
    task5 = PythonOperator(
        task_id='task5',
        python_callable=choose_model,
        trigger_rule="all_success"
    )
    
task0.as_setup() >> task1
task1 >> [task2, task3]
task3 >> group_4 >> task5
