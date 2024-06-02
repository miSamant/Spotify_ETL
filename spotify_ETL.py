
################################################################################
# Filename: spotify_ETL.py
# Author: Mihir Samant
# Date: 2024-06-02
# Requirements:
#    - DB
#    - airflow
################################################################################

import pandas as pd 
import json
from datetime import timedelta
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.mysql_hook import MySqlHook
from sqlalchemy import create_engine
from airflow.hooks.base_hook import BaseHook
import requests
from airflow.models import Variable
from tabulate import tabulate
import pytz

# Setting up variables
conn = BaseHook.get_connection("spotify_api")
client_id = conn.login
client_secret = conn.password
redirect_uri = Variable.get("spotify_redirect_uri")
auth_code = Variable.get("spotify_auth_code")
EST = pytz.timezone("Canada/Eastern") # check your timezones https://gist.github.com/heyalexej/8bf688fd67d7199be4a1682b3eec7568
my_db = 'YOUR_DB_NAME_ON_AIRFLOW' # Enter the name of your DB which you have saved under connection in Airflow UI

def _exchange_token():
    """
    This function is responsible for exchanging tokens and setting up variables in Airflow
    """
    auth_url = 'https://accounts.spotify.com/api/token'
    auth_payload = {
        'grant_type': 'authorization_code',
        'code': auth_code,
        'redirect_uri': redirect_uri,
        'client_id': client_id,
        'client_secret': client_secret
    }
    print(auth_code)
    response = requests.post(auth_url, data=auth_payload)

    if response.status_code == 200:
        access_token = response.json().get('access_token')
        refresh_token = response.json().get('refresh_token')
        Variable.set("spotify_access_token", access_token)
        Variable.set("spotify_refresh_token", refresh_token)
        print(f'Access token obtained: {access_token}')
        print(f'Refresh token obtained: {refresh_token}')
    else:
        print(f'Failed to obtain access token. Response: {response.json()}')

def _refresh_token():
    """
    This function is responsible for refreshing tokens and setting up variables in Airflow
    """
    refresh_token = Variable.get("spotify_refresh_token")
    auth_url = 'https://accounts.spotify.com/api/token'
    auth_payload = {
        'grant_type': 'refresh_token',
        'refresh_token': refresh_token,
        'client_id': client_id,
        'client_secret': client_secret
    }
    print(refresh_token)
    response = requests.post(auth_url, data=auth_payload)
    # auth_tkn = requests.get("https://accounts.spotify.com/authorize?client_id=bce6649eb8984381b9955ea9b81486f2&response_type=code&redirect_uri=http://localhost:8888/callback&scope=user-read-recently-played")
    # print(auth_tkn.text)
    if response.status_code == 200:
        access_token = response.json().get('access_token')
        Variable.set("spotify_access_token", access_token)
        print(f'New access token obtained: {access_token}')
    else:
        print(f'Failed to refresh access token. Response: {response.json()}')

def _get_recently_played_tracks():
    """
    This function is responsible for pulling the last played tracks in last 24H and push them on the MySQL DB
    """
    access_token = Variable.get("spotify_access_token")
    headers = {
        'Authorization': f'Bearer {access_token}',
    }
    yesterday = datetime.now(EST)-timedelta(days=1)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000
    # print(time)
    recently_played_url = f'https://api.spotify.com/v1/me/player/recently-played?limit=50&after={yesterday_unix_timestamp}'
    print(recently_played_url)
    response = requests.get(recently_played_url, headers=headers)

    if response.status_code == 200:
        recently_played_tracks = response.json()

    else:
        print(f'Failed to get recently played tracks. Response: {response.json()}')

    data = recently_played_tracks

    song_names = []
    artist_names = []
    played_at_list = []
    timestamps = []

    # Extracting only the relevant bits of data from the json object      
    for song in data["items"]:
        song_names.append(song["track"]["name"])
        artist_names.append(song["track"]["album"]["artists"][0]["name"])
        played_at_list.append(song["played_at"])
        timestamps.append(song["played_at"][0:10])
        
    # Prepare a dictionary in order to turn it into a pandas dataframe below       
    song_dict = {
        "song_name" : song_names,
        "artist_name": artist_names,
        "played_at" : played_at_list,
        "timestamp" : timestamps
    }

    song_df = pd.DataFrame(song_dict, columns = ["song_name", "artist_name", "played_at", "timestamp"])
    print(tabulate(song_df))
    print(song_df)

    sql_query = """
    CREATE TABLE IF NOT EXISTS my_played_tracks(
        song_name VARCHAR(200),
        artist_name VARCHAR(200),
        played_at VARCHAR(200),
        timestamp VARCHAR(200),
        CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
    )
    """
    connection = MySqlHook.get_connection(my_db)  
    con_str = 'mysql://'+ str(connection.login)+':'+str(connection.password) + '@' + str(connection.host) +':'+str(connection.port)+'/'+str(connection.schema)
    engine = create_engine(con_str) # Creating engine with the con_str (We are pulling all the variables with the help of the MySqlHook

    # Creating Table (This will run only once)
    try:
        engine.execute(sql_query)
        print("Table created successfully.")
    except:
        print(f"Table Already present pushing data into DB")

    # Loading df into DB   
    try:
        song_df.to_sql("my_played_tracks", engine, index=False, if_exists='append')
    except:
        print("Data already exists in the database")

    engine.dispose() # Closing the engine 

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 29),
    'retries': 1,
    'retry_delay': timedelta(minutes=60),

}
with DAG(
    'spotify_dag',
    default_args=default_args,
    schedule_interval='10 4 * * *',  # This DAG will run at 10 min past mindnight (12:10)EST, The rason I have build 4:10 because my airflow follows UTC timestamp
):

    exchange = PythonOperator(
        task_id = 'exchange_token',
        python_callable = _exchange_token
    )

    refresh = PythonOperator(
        task_id = 'refresh_token',
        python_callable = _refresh_token
    )

    get_songs = PythonOperator(
        task_id= 'get_songs',
        python_callable = _get_recently_played_tracks
    )

# Dependancies 
exchange >> refresh >> get_songs