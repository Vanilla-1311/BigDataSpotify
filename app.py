from flask import Flask, render_template
from flask_pymongo import PyMongo
from pymongo import MongoClient
import os

app = Flask(__name__)
def get_db():
    client = MongoClient('mongodb://mongodb:27017/')
    db = client['StoreOfCategory']
    return db


@app.route('/')
def index():
    db = get_db()
    tracks = db.tracks.find() 
    return render_template('index.html', tracks=tracks)

if __name__ == "__main__":
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=5000)
