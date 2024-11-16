from flask import Flask, render_template
from pymongo import MongoClient

app = Flask(__name__)

# MongoDB-Verbindungsinformationen
client = MongoClient("mongodb://localhost:27017")
db = client["StoreOfCategory"]  # Ersetze 'mydatabase' durch den tatsächlichen Namen deiner Datenbank


@app.route('/')
def index():
    # Daten aus der MongoDB-Sammlung abrufen
    tracks = db.tracks.find()  # Ersetze 'tracks' durch den tatsächlichen Namen der Sammlung
    return render_template('index.html', tracks=tracks)

if __name__ == '__main__':
    app.run(debug=True)
