# **Big Data Praxisleistung: Use Spotify Audio Features To Categorize Music**

**Autor:** Vanessa Wettori  
**Institution:** DHBW Stuttgart

Der ETL-Workflow nutzt Apache Airflow, um Daten aus der Spotify-API zu extrahieren, diese zu verarbeiten und anschließend in einer MongoDB-Datenbank zu speichern. Der Nutzer gibt eine Spotify-Playlist-ID ein, anhand der die enthaltenen Tracks geladen werden. Für jeden Track werden zusätzlich die zugehörigen Audio-Features abgerufen. Auf Basis dieser Audio-Features werden die Songs automatisch einer Kategorie wie beispielsweise HipHop zugeordnet. Die Ergebnisse lassen sich anschließend über eine Weboberfläche übersichtlich anzeigen.

##  **Inhaltsverzeichnis**


- [Projektstruktur](#Projektstruktur)
- [Airflow](#Airflow)
- [Weitere verwendete Skripte](#weitere-verwendete-skripte)
- [Ausführung](#Ausführung)

## Projektstruktur

Von nicht relevanten Ordnern wurde darauf verzichtet, die Struktur und Inhalte anzugeben.
Die Projektstrutur sieht wie folgt aus:

```bash
├─ Dockerfile.airflow
├─ Dockerfile.flask
├─ README.md
├─ app.py
├─ dags
│  ├─ __pycache__
│  └─ track_categorization.py
├─ data
│  └─ mongodb/
├─ docker-compose.yml
├─ playlist
│  └─ playlist_id.txt
├─ plugins/
├─ requirements.tx
├─ set_playlist_id.py
├─ startup.sh
└─ templates
   └─ index.html
```

### Erklärung der Struktur

- **`Dockerfile.airflow`**: Die Konfiguration für das Airflow-Docker-Image
- **`Dockerfile.flask`**: Die Konfiguration für das Flask-Docker-Image
- **`README.md`**: Die Dokumentation des Projekts.
- **`app.py`**: - Die Datei erstellt eine Flask-Webanwendung, die eine Verbindung zu einer MongoDB-Datenbank herstellt und eine HTML-Seite rendert, die alle Tracks aus der Datenbank anzeigt.
- **`dags/`**: Enthält die DAG die für den Workflow zuständig ist.
  - **`track_categorization`**: Enthält den Airflow-DAG um die Daten von Spotify zu ziehen und verarbeitet diese weiter.
- **`data/`**: Verzeichnis für die Daten:
  - **`mongodb/`**: JSON mit Key für die Kaqqle API.
- **`docker-compose.yml`**: Verwaltet die verschiedenen Container.
- **`playlist`**: Ordner, welches eine Textdatei mit der Playlist-Id enthält, die vom User eingegeben wurde.
- **`plugins/`**: Eine Erweiterungen für Airflow
- **`requirements.txt`**: Bibliotheken die für das Projekt benötigt werden.
- **`set_playlist_id.py`**: Skript, mit dem der User die Playlist-Id setzen kann.
- **`startup.sh`**: Startet den Airflow-Scheduler und den Webserver.
-**`templates`**: Enthält das Frontend, um die Ergebnisse aus MongoDB visuell darzustellen.
  -**`index.html`**: Frontend für die Ergenisee

## **Airflow**

Die Airflow-DAG besteht aus 6 Tasks, die im Folgenden weiter erklärt werden. Die Reihenfolge entspricht der hier aufgeführten Nummerierung.

### **Ablauf des Workflows**

1. **Erstellen und leeren der Verzeichnisse: `clear_directorys`**

   - Die Task `clear_directorys` ist ein BashOperator, der vorhandene Verzeichnisse löscht undneue Verzeichnisse auf dem HDFS (Hadoop Distributed File System)  erstellt, um sicherzustellen, dass alte Daten den Workflow nicht beeinflussen.

2. **Leeren der Datenbank: `clear_database`**

   - Die Task `clear_database` ist ein PythonOperator, der in der Airflow-DAG definiert ist. Er ruft die Funktion `clear_mongo_data` auf, um die tracks-Kollektion in der MongoDB-Datenbank zu löschen. Dies stellt sicher, dass alte Daten entfernt werden und die Datenbank für neue Daten vorbereitet ist.

3. **Herunterladen der Track-IDs: `get_playlist_tracks`**

   - Die Task `get_playlist_tracks` ist ein BashOperator, der die Spotify-API aufruft, um die Tracks einer Playlist abzurufen. Die Ergenisse werden als JSON-Datei im HDFS (Hadoop Distributed File System) gespeichert. Dabei wird zunächst ein Zugriffstoken von der Spotify-API angefordert und anschließend die Tracks der Playlist abgerufen. Die Playlist-ID wurde vor Containerstart von dem User eingegeben und wird zum Start der ETL aus dem Textdatei in eine Variable eingelesen, die Task verwendet.

4. **Herunterladen der Audiofeatures für die zuor gefundenen Tracks: `get_audio_features`**

   - Die Task `get_audio_features` ist ein PythonOperator, der die Funktion `get_for_each_track_audio_features` aufruft. Diese Funktion dient dazu, die Audio-Features für jeden Track einer Playlist abzurufen und als JSON-Dateien im HDFS zu speichern. Dabei liest `get_for_each_track_audio_features` die Track-IDs aus einer JSON-Datei, die die Playlist-Daten enthält, und ruft die entsprechenden Audio-Features über die Spotify-API ab.

5. **Speichern im finalen Verzeichnis: `save_to_final_path`**

   - Die Task `save_to_final_path` ist ein PythonOperator, der die Funktion `save_tracks_to_final_path` aufruft, um die abgerufenen Track- und Audio-Feature-Daten zu verarbeiten und die bereinigten Daten als Parquet-Dateien im HDFS zu speichern.

6. **Kategorien berechnen und in Datenbank speichern: `calculate_category_and_save_to_db`**

   - Die Task `calculate_category_and_save_to_db` ist ein PythonOperator, der die Funktion calculate_category aufruft, um die Audio-Features der Tracks zu analysieren. Die Tracks werden mithilfe von Apache Spark, in verschiedene Kategorien eingeordnet und die Ergebnisse in einer MongoDB-Datenbank zu gespeichert. Diese ist die endgültige Datenbank.

## Weitere verwendete Skripte

Wie schon in dem Kaptiel Ordnerstruktur erwähnt, gibt es noch zwei weitere Python-Skripte, die die Funktionweise von der ETL sicherstellen. Sie sind für die Ausführung und Darstellung der Ergebnisse wichtig.

1. **`set_playlist_id.py`**:
   - Das Skript ermöglicht es dem Benutzer, eine Playlist-ID einzugeben und diese in einer Datei zu speichern. Diese Playlist-ID wird später von der Airflow-DAG verwendet, um die Tracks der angegebenen Playlist von der Spotify-API abzurufen. Dazu erstellt das Skript eine Text-Datei, die die Playlist-ID enthält. Diese wird in dem Verzeichnis `Playlist` gespeichert, worauf die DAG zugriff hat und dieses auslesen kann.
  
2. **`app.py`**:
   - Das Skript erstellt eine Flask-Webanwendung, die eine Verbindung zu einer MongoDB-Datenbank herstellt und eine HTML-Seite rendert, die alle Tracks aus der Datenbank anzeigt. Somit wird dadurch die Weboberfläche bereitgestellt und sichergestellt, dass eine Verbindung zu der Datenbank besteht.

## **Ausführung**

1. **Voraussetzungen:**

   - Docker und Docker-Compose installiert

2. **Schritte zur Ausführung:**

```bash
git clone https://github.com/Vanilla-1311/BigDataSpotify.git
cd BigData
```

Nach diesen Befehlen muss zunächst `set_playlist_id.py` ausgeführt werden. Dies geschieht mithilfe diesen Befehls:

```bash
python set_playlist_id.py
```

Hier wird man aufgefordert, die Playlist-ID einzugeben. Danach kann der Conatiner gestartet werden.

```bash
docker-compose up
```

### 3. Hadoop Cluster starten

```bash
docker exec -it hadoop bash
sudo su hadoop
start-all.sh
```

### 4. Airflow-API öffnen

Auf `localhost:8080` kann die Airflow-Api jetzt geöffnet werden, um die DAG zu starten.

### 5. Flask-Frontend öffnen

Im Browser unter `localhost:5000` kann nach der Vollendigung der Aufgabe, die Ergebnisse betrachtet werden. 