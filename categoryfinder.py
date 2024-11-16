import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import when

# Beispiel-Daten: DataFrame mit Audio-Features für die Tracks
data = {
    'title': ['Song1', 'Song2', 'Song3', 'Song4', 'Song5'],
    'energy': [0.8, 0.3, 0.6, 0.4, 0.9],
    'tempo': [130, 85, 120, 95, 125],
    'acousticness': [0.1, 0.8, 0.2, 0.5, 0.3],
    'speechiness': [0.05, 0.1, 0.6, 0.9, 0.35],
    'danceability': [0.7, 0.4, 0.6, 0.3, 0.8],
    'valence': [0.6, 0.7, 0.4, 0.5, 0.5],
    'loudness': [-5, -10, -6, -8, -3]
}

# DataFrame erstellen
df = pd.DataFrame(data)

# Funktion zur Kategorisierung eines Tracks basierend auf Audio-Features
def categorize_track(row):
    if row['energy'] > 0.7 and row['tempo'] > 120 and row['acousticness'] < 0.3:
        return 'Metal'
    elif row['acousticness'] > 0.7 and row['energy'] < 0.5 and row['tempo'] < 100:
        return 'Classic'
    elif row['energy'] > 0.6 and 100 <= row['tempo'] <= 140 and 0.5 <= row['danceability'] <= 0.7:
        return 'Rock'
    elif row['speechiness'] > 0.4:
        return 'Vocal'
    elif row['energy'] > 0.6 and row['tempo'] > 120 and row['danceability'] > 0.7:
        return 'Electro'
    elif row['speechiness'] > 0.8:
        return 'Podcast'
    elif row['valence'] > 0.5 and 0.5 <= row['danceability'] <= 0.7 and 0.4 <= row['energy'] <= 0.6:
        return 'Soul'
    elif row['speechiness'] > 0.3:
        return 'HipHop'
    else:
        return 'Other'

# Kategorisierung anwenden
df['category'] = df.apply(categorize_track, axis=1)

# Ausgabe des kategorisierten DataFrames
print(df[['title', 'category']])



# Spark-Sitzung erstellen
spark = SparkSession.builder.appName("MusicCategorization").getOrCreate()

# 1. Daten laden
track_data = spark.read.json("hdfs:///path/to/track_data.json")
audio_features = spark.read.json("hdfs:///path/to/audio_features.json")

# 2. Kategorien berechnen
audio_features = audio_features.withColumn(
    "category",
    when((audio_features["energy"] > 0.7) & (audio_features["tempo"] > 120) & (audio_features["acousticness"] < 0.3), "Metal🤘")
    .when(audio_features["acousticness"] > 0.7 & audio_features["energy"] < 0.5 & audio_features["tempo"] < 100, "Classic🏛️")
    .when(audio_features["speechiness"] > 0.8, "Podcast🎧")
    .when(audio_features["speechiness"] > 0.4, "Vocal🎤")
    .when((audio_features["energy"] > 0.6) & (audio_features["tempo"] > 120) & (audio_features["danceability"] > 0.7), "Electro⚡")
    .when(audio_features["energy"] > 0.6 & (audio_features["tempo"] > 100) & (audio_features["tempo"] < 140) & (audio_features["danceability"] > 0.5), "HipHop🎤")
    .when((audio_features["valence"] > 0.5) & (audio_features["danceability"] > 0.5) & (audio_features["danceability"] < 0.7) & (audio_features["energy"] > 0.4) & (audio_features["energy"] < 0.6), "Soul🎷")
    .when(audio_features["energy"] > 0.6 & (audio_features["tempo"] > 100) & (audio_features["tempo"] < 140) & (audio_features["danceability"] > 0.5) & (audio_features["danceability"] < 0.7), "Rock🎸")
          
    .otherwise("Other")
)

# 3. Daten zusammenführen
merged_data = track_data.join(audio_features, on="id")

# 4. Ergebnisse in MongoDB speichern
merged_data.write.format("mongo").option("uri", "mongodb://localhost:27017/StoreOfCategory/tracks").mode("append").save()

print("Daten erfolgreich in MongoDB gespeichert.")