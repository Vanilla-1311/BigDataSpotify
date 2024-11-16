# pylint: disable=all


from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime
import glob
import pandas as pd
import json
from pymongo import MongoClient
from pyspark.sql import SparkSession
from pyspark.sql.functions import when
from pymongo import MongoClient

# Standard-Playlist-ID, falls keine beim Start übergeben wird
DEFAULT_PLAYLIST_ID = Variable.get("playlist_id", default_var="3cEYpjA9oz9GiPac4AsH4n")

def get_for_each_track_audio_features():
    json_file = glob.glob('/user/hadoop/spotify/track_data/raw/playlist_tracks.json')
    
    with open(json_file, 'r') as file:
        tracks_data = json.load(file)
    
    # Beispiel: Zugriff auf die Track-Daten (kann weiter angepasst werden)
    for i, item in enumerate(tracks_data['items']):
        track = item['track']
        track_id = track['id']
    
        task = BashOperator(
            task_id=f'get_track_details_{track_id}',
            bash_command=f"""
                ACCESS_TOKEN="{{ ti.xcom_pull(task_ids='get_access_token') }}"
                curl -X "GET" "https://api.spotify.com/v1/audio-features/{track_id}" \
                    -H "Accept: application/json" \
                    -H "Content-Type: application/json" \
                    -H "Authorization: Bearer {{ ti.xcom_pull(task_ids='get_access_token') }}" \
                -o /user/hadoop/spotify/track_data/raw/track_data_{track_id}.json
            """,
            
        )
    

def read_all_tracks_as_dataframe():
    # Pfad zu den JSON-Dateien
    json_files = glob.glob('/user/hadoop/spotify/track_data/raw/playlist_track.json')
    
    # Liste zum Speichern der einzelnen DataFrames
    dataframes = []
    
    # Jede JSON-Datei einlesen und zur Liste hinzufügen
    for file in json_files:
        track_data = pd.read_json(file)
        # Extrahiere die gewünschten Spalten aus der verschachtelten Struktur
        filtered_data = pd.DataFrame({
            'album_type': track_data['album'].apply(lambda x: x['album_type']),
            'name': track_data['name'],
            'release_date': track_data['album'].apply(lambda x: x['release_date']),
            'artist_name': track_data['album'].apply(lambda x: x['artists'][0]['name'])
        })
        dataframes.append(filtered_data)
    
    # Alle DataFrames zu einem einzigen DataFrame zusammenführen
    all_tracks_df = pd.concat(dataframes, ignore_index=True)
    
    return all_tracks_df

def read_all_audio_features_as_dataframe():
    json_files = glob.glob('/user/hadoop/spotify/audio_features/raw/audio_features_*.json')
    
    # Liste zum Speichern der einzelnen DataFrames
    dataframes = []
    
    # Jede JSON-Datei einlesen und zur Liste hinzufügen
    for file in json_files:
        audio_features_data = pd.read_json(file)
        # Nur die gewünschten Spalten auswählen
        filtered_data = audio_features_data[[
            'danceability', 'energy', 'loudness', 'speechiness', 
            'acousticness', 'instrumentalness', 'liveness', 
            'valence', 'tempo', 'duration_ms'
        ]]
        dataframes.append(filtered_data)
    
    # Alle DataFrames zu einem einzigen DataFrame zusammenführen
    all_audio_features_df = pd.concat(dataframes, ignore_index=True)
    
    return all_audio_features_df

def process_dataframes(ti):
    tracks_df = ti.xcom_pull(task_ids='read_track_info')
    audio_features_df = ti.xcom_pull(task_ids='read_audio_features')
    # Speichere die DataFrames als Parquet-Dateien
    tracks_df.to_parquet('/user/hadoop/spotify/track_data/final/tracks.parquet', index=False)
    audio_features_df.to_parquet('/user/hadoop/spotify/audio_features/final/audio_features.parquet', index=False)
    #TODO: Inkrement hinzufügen

def calculate_category():
    spark = SparkSession.builder.appName("MusicCategorization").getOrCreate()
    tracks_df = spark.read.parquet('/user/hadoop/spotify/track_data/final/tracks.parquet')
    audio_features_df = spark.read.parquet('/user/hadoop/spotify/audio_features/final/audio_features.parquet')
    
    # Berechnung der Kategorien basierend auf den Audio-Features
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
        .otherwise("Other")
    )
    
    # Zusammenführen der DataFrames
    merged_df = tracks_df.join(audio_features_df, on='name')
    merged_df.write.format("mongo").option("uri", "mongodb://localhost:27017/StoreOfCategory/tracks").mode("append").save()

dag = DAG(
    'spotify_etl_pipeline',
    start_date=datetime(2024, 11, 11),
    schedule_interval='@daily',
    params={"playlist_id": DEFAULT_PLAYLIST_ID}
)

# Task 1
get_access_token = BashOperator(
    task_id='get_access_token',
    bash_command="""
    curl -X POST "https://accounts.spotify.com/api/token" \
        -H "Content-Type: application/x-www-form-urlencoded" \
        -d "grant_type=client_credentials" \
        -d "client_id=d92387db27094e1185f12bd301e911f0" \
        -d "client_secret=3016739d849245609bbb99010c18532e" \
        | jq -r '.access_token'
    """,
    do_xcom_push=True  # Aktiviert das Speichern der Ausgabe in XComs
)

# Task 2
get_playlist_tracks = BashOperator(
    task_id='get_playlist_tracks',
    bash_command="""
    ACCESS_TOKEN="{{ ti.xcom_pull(task_ids='get_access_token') }}"
    curl -X "GET" "https://api.spotify.com/v1/playlists/{{ dag_run.conf['playlist_id'] }}/tracks" \
        -H "Accept: application/json" \
        -H "Content-Type: application/json" \
        -H "Authorization: Bearer $ACCESS_TOKEN" \
        | jq '[.items[].track]' > /user/hadoop/spotify/track_data/raw/playlist_tracks.json
    """,   
)
    

# Task 3
    # get track id from /user/hadoop/spotify/track_data/raw/…
    # get track information from Spotify API
    # save track information to /user/hadoop/spotify/audio_features/…
get_track_info = BashOperator(
    task_id='get_track_info',
    bash_command="""
    ACCESS_TOKEN="{{ ti.xcom_pull(task_ids='get_access_token') }}"
    curl -X "GET" "https://api.spotify.com/v1/tracks/2lEcSduKEXEK5KJ9hJzlCz?market=DE" \
        -H "Accept: application/json" \
        -H "Content-Type: application/json" \
        -H "Authorization: Bearer {{
        ti.xcom_pull(task_ids='get_access_token') }}
        hdfs dfs -put -f - /user/hadoop/spotify/track_data/raw/track_data_{track_id}.json"
    """
)   
#Task 3
    # get audio features from Spotify API
    # save audio features to /user/hadoop/spotify/audio_features/raw 
'''
get_audio_features = BashOperator(
    task_id='get_audio_features',
    bash_command="""
    curl -X "GET" "https://api.spotify.com/v1/audio-features/2lEcSduKEXEK5KJ9hJzlCz" \
        -H "Accept: application/json" \
        -H "Content-Type: application/json" \
        -H "Authorization: Bearer {{
        ti.xcom_pull(task_ids='get_access_token') }} \
        hdfs dfs -put -f - /user/hadoop/spotify/audio_features/raw/audio_features_{track_id}.json"
    """)
'''

#Task 4
read_tracka_as_panda_frame = PythonOperator(
    task_id='read_track_info',
    python_callable=read_all_tracks_as_dataframe,
)

#Task5    
read_audio_features_as_panda_frame = PythonOperator(
    task_id='read_audio_features',
    python_callable=read_all_audio_features_as_dataframe,
)

#Task6
process_data = PythonOperator(
    task_id='process_data',
    python_callable=process_dataframes,
    provide_context=True, 
)

#Task7
calculate_category_and_save_to_db = PythonOperator(
    task_id='calculate_category',
    python_callable=calculate_category,
)

 

        


# Airflow Tasks
get_access_token >> get_track_info >> get_audio_features >> read_tracka_as_panda_frame >> read_audio_features_as_panda_frame >> process_data >> calculate_category_and_save_to_db 
