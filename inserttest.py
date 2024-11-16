from pymongo import MongoClient

# Verbindung zur MongoDB
client = MongoClient("mongodb://localhost:27017")
db = client["StoreOfCategory"]  # Ersetze 'mydatabase' durch deinen tatsächlichen Datenbanknamen
collection = db["Tracks"]  # Ersetze 'tracks' durch den tatsächlichen Namen der Sammlung

# Test-Tracks-Daten
test_tracks = [
    {
        "title": "Test Song 1",
        "artist": "Test Artist 1",
        "category": "Rock",
        "audio_features": {
            "danceability": 0.7,
            "energy": 0.8,
            "tempo": 120
        }
    },
    {
        "title": "Test Song 2",
        "artist": "Test Artist 2",
        "category": "HipHop",
        "audio_features": {
            "danceability": 0.9,
            "energy": 0.6,
            "tempo": 100
        }
    },
    {
        "title": "Test Song 3",
        "artist": "Test Artist 3",
        "category": "Classic",
        "audio_features": {
            "danceability": 0.3,
            "energy": 0.4,
            "tempo": 60
        }
    }
]

# Einfügen der Testdaten
collection.insert_many(test_tracks)
print("Drei Test-Tracks erfolgreich eingefügt.")
