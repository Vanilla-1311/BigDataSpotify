# pylint: disable=all


from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import glob
import pandas as pd
import json
from pymongo import MongoClient
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col
from pymongo import MongoClient
import base64
import os
import time

os.environ['PYTHONIOENCODING'] = 'utf-8'



default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

def clear_mongo_data():
    client = MongoClient("mongodb://mongodb:27017/")
    db = client["StoreOfCategory"]
    db.tracks.drop()
    
def get_playlist_id_from_file():
    with open("/home/airflow/airflow/playlist/playlist_id.txt", "r", encoding="utf-8") as file:
        playlist_id = file.read()
    return playlist_id

def get_for_each_track_audio_features():
    json_file = '/user/hadoop/spotify/track_data/raw/playlist_tracks.json'
    
    with open(json_file, 'r', encoding='utf-8') as file:
        tracks_data = json.load(file)
      
    for i, item in enumerate(tracks_data['items']):
        track = item['track']
        track_id = track['id']
         
        last_saved_file = f'/user/hadoop/spotify/audio_features/raw/audio_features_{track_id}.json'
        
        task = BashOperator(
            task_id=f'get_track_details_{track_id}',
            bash_command=f"""
                CLIENT_ID='d92387db27094e1185f12bd301e911f0'
                CLIENT_SECRET='3016739d849245609bbb99010c18532e'
                CLIENT_ID_SECRET=$(echo -n "$CLIENT_ID:$CLIENT_SECRET" | base64 -w 0)
                ACCESS_TOKEN=$(curl --location --request POST 'https://accounts.spotify.com/api/token' \
                --header 'Authorization: Basic '$CLIENT_ID_SECRET \
                --header 'Content-Type: application/x-www-form-urlencoded' \
                --data-urlencode 'grant_type=client_credentials' | jq -r '.access_token')
                
                curl -X "GET" "https://api.spotify.com/v1/audio-features/{track_id}" \
                    -H "Accept: application/json" \
                    -H "Content-Type: application/json" \
                    -H "Authorization: Bearer $ACCESS_TOKEN" \
                -o {last_saved_file}
            """,
            dag=dag,
        )
        if i % 2 == 0:
            time.sleep(1)
        task.execute(context={})

def save_tracks_to_final_path():
    json_files = '/user/hadoop/spotify/track_data/raw/playlist_tracks.json'
    
    with open(json_files, 'r',  encoding='utf-8') as file:
        data = json.load(file)
        
    track_list = []
    
    for i, item in enumerate(data['items']):
        track = item['track']
        album = track['album']
        artist = album['artists'][0]
        
        track_info = {
            'track_id': track['id'],
            'album_type': album['album_type'],
            'name': track['name'],
            'release_date': album['release_date'],
            'artist_name': artist['name']
        }
        track_infos = pd.DataFrame([track_info])
        track_list.append(track_infos)
    
    df_fresh_created = pd.concat(track_list, ignore_index=True)
    

    audio_files = glob.glob('/user/hadoop/spotify/audio_features/raw/audio_features_*.json')
    

    dataframes = []

    for file in audio_files:
        with open(file, 'r', encoding='utf-8') as f:
            audio_features_data = json.load(f)

           
            if isinstance(audio_features_data, dict):
                audio_features_data = [audio_features_data]  

            for entry in audio_features_data:
                entry['track_id'] = entry.get('id')  

            df = pd.DataFrame(audio_features_data)
            
            required_fields = ['acousticness', 'speechiness', 'tempo', 'liveness', 'loudness', 'duration_ms', 'danceability', 'instrumentalness', 'energy', 'valence']
            
            if all(field in audio_features_data[0] for field in required_fields):
                for entry in audio_features_data:
                    entry['track_id'] = entry.get('id')  
                    
                df = pd.DataFrame(audio_features_data)
                
        
                filtered_data = df[[
                    'track_id',  
                    'danceability', 'energy', 'loudness', 'speechiness', 
                    'acousticness', 'instrumentalness', 'liveness', 
                    'valence', 'tempo', 'duration_ms'
                ]]
            
                dataframes.append(filtered_data)


    combined_df = pd.concat(dataframes, ignore_index=True)
    
    df_fresh_created.to_parquet('/user/hadoop/spotify/track_data/final/tracks.parquet', index=False)
    combined_df.to_parquet('/user/hadoop/spotify/audio_features/final/audio_features.parquet', index=False)
    
    return df



def calculate_category():
    spark = SparkSession.builder \
    .appName("MusicCategorization") \
    .config("spark.mongodb.input.uri", "mongodb://mongodb:27017/StoreOfCategory.tracks") \
    .config("spark.mongodb.output.uri", "mongodb://mongodb:27017/StoreOfCategory.tracks") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.11:2.3.4") \
    .getOrCreate()
    
    
    tracks_df = spark.read.parquet('file:///user/hadoop/spotify/track_data/final/tracks.parquet')
    audio_features_df = spark.read.parquet('file:///user/hadoop/spotify/audio_features/final/audio_features.parquet')
  
    audio_features_df = audio_features_df.withColumn(
        "category", 
        when((audio_features_df["energy"] > 0.7) & (audio_features_df["tempo"] > 120) & (audio_features_df["acousticness"] < 0.3), "Metal🤘")
        .when((audio_features_df["acousticness"] > 0.7) & (audio_features_df["energy"] < 0.5) & (audio_features_df["tempo"] < 100), "Classic🏛️")
        .when(audio_features_df["speechiness"] > 0.8, "Podcast🎧")
        .when(audio_features_df["speechiness"] > 0.4, "Vocal🎤")
        .when((audio_features_df["energy"] > 0.6) & (audio_features_df["tempo"] > 120) & (audio_features_df["danceability"] > 0.7), "Electro⚡")
        .when((audio_features_df["energy"] > 0.6) & (audio_features_df["tempo"] > 100) & (audio_features_df["tempo"] < 140) & (audio_features_df["danceability"] > 0.5), "HipHop🎤")
        .when((audio_features_df["valence"] > 0.5) & (audio_features_df["danceability"] > 0.5) & (audio_features_df["danceability"] < 0.7) & (audio_features_df["energy"] > 0.4) & (audio_features_df["energy"] < 0.6), "Soul🎷")
        .when((audio_features_df["energy"] > 0.6) & (audio_features_df["tempo"] > 100) & (audio_features_df["tempo"] < 140) & (audio_features_df["danceability"] > 0.5) & (audio_features_df["danceability"] < 0.7), "Rock🎸")
        .otherwise("Other🦦")
    )
    
    
    
    tracks_df = tracks_df.withColumn("track_id", col("track_id").cast("string"))
    audio_features_df = audio_features_df.withColumn("track_id", col("track_id").cast("string"))

    
    # Zusammenführen der DataFrames
    merged_df = tracks_df.join(audio_features_df, on='track_id', how='inner')
    merged_df.write.format("mongo").mode("append").save()


dag = DAG(
    'spotify_etl_pipeline',
    start_date=datetime(2024, 11, 11),
    schedule_interval=None,
)

client_id = "d92387db27094e1185f12bd301e911f0"
client_secret = "3016739d849245609bbb99010c18532e"
client_credentials = f"{client_id}:{client_secret}"
encoded_credentials = base64.b64encode(client_credentials.encode()).decode()
PLAYLIST_ID = get_playlist_id_from_file()

# Task 1
clear_directorys = BashOperator(
    task_id='clear_directorys',
    bash_command="""
    hdfs dfs -rm -r /user/hadoop/spotify/track_data/raw
    hdfs dfs -rm -r /user/hadoop/spotify/track_data/final
    hdfs dfs -rm -r /user/hadoop/spotify/audio_features/raw
    hdfs dfs -rm -r /user/hadoop/spotify/audio_features/final
    hdfs dfs -mkdir -p /user/hadoop/spotify/audio_features/raw
    hdfs dfs -mkdir -p /user/hadoop/spotify/audio_features/final
    hdfs dfs -mkdir -p /user/hadoop/spotify/track_data/raw
    hdfs dfs -mkdir -p /user/hadoop/spotify/track_data/final
    
    """,
    dag=dag
)

# Task 2
clear_database = PythonOperator(
    task_id=f'clear_database',
    python_callable=clear_mongo_data,
    dag=dag,
)

# Task 3
get_playlist_tracks = BashOperator(
    task_id='get_playlist_tracks',
    bash_command=f"""
    curl -I https://accounts.spotify.com/api/token
    export LANG=en_US.UTF-8

    CLIENT_ID='d92387db27094e1185f12bd301e911f0'
    CLIENT_SECRET='3016739d849245609bbb99010c18532e'
    CLIENT_ID_SECRET=$(echo -n "$CLIENT_ID:$CLIENT_SECRET" | base64 -w 0)
    PLAYLIST_ID="{PLAYLIST_ID}"
    
    ACCESS_TOKEN=$(curl --location --request POST 'https://accounts.spotify.com/api/token' \
    --header 'Authorization: Basic '$CLIENT_ID_SECRET \
    --header 'Content-Type: application/x-www-form-urlencoded' \
    --data-urlencode 'grant_type=client_credentials' | jq -r '.access_token')
    
    curl -X "GET" "https://api.spotify.com/v1/playlists/$PLAYLIST_ID/tracks" \
        -H "Accept: application/json" \
        -H "Content-Type: application/json" \
        -H "Authorization: Bearer $ACCESS_TOKEN" \
        > /user/hadoop/spotify/track_data/raw/playlist_tracks.json
    echo "Playlist tracks fetched and saved to HDFS"
    """,  
    dag=dag 
)
    
#Task 4
get_audio_features = PythonOperator(
    task_id='get_audio_features',
    python_callable=get_for_each_track_audio_features,
    dag=dag
)

#Task 5
save_to_final_path = PythonOperator(
    task_id='read_track_info',
    python_callable=save_tracks_to_final_path,
    dag=dag
)

#Task 6
calculate_category_and_save_to_db = PythonOperator(
    task_id='calculate_category',
    python_callable=calculate_category,
    dag=dag
)
        


# Airflow Tasks
clear_directorys >> clear_database >> get_playlist_tracks >> get_audio_features >> save_to_final_path >> calculate_category_and_save_to_db
