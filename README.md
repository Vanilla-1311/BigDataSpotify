# **Big Data Praxisleistung: Use Spotify Audio Features To Categorize Music**

**Autor:** Vanessa Wettori  
**Institution:** DHBW Stuttgart

Dieses Projekt analysiert Hubway Bike-Sharing-Daten und erstellt ein Excel-File mit Dashboards, das zentrale **Kennzahlen (KPIs)** visualisiert.  
Es verwendet **Docker** und **Apache Airflow**, um eine automatisierte ETL-Pipeline (Extrahieren, Transformieren, Laden) zu implementieren.  
Das Projekt wurde als **Projektabgabe für die Vorlesung Big Data** angefertigt.

## 🗂 **Inhaltsverzeichnis**

- [Berechnete KPIs](#Berechnete-KPIs)
- [Projektstruktur](#Projektstruktur)
- [Airflow](#Airflow)
- [Funktionen](#Funktionen)
- [Ausführung](#Ausführung)

## **Berechnete-KPIs**

Jede der folgenden **Kennzahlen** wird monatlich berechnet und einzelnd über ein Dashboard visualisiert:

### **Allgemeine Statistiken**

- Durchschnittliche Fahrtdauer (in Minuten)
- Durchschnittliche Fahrdistanz (in Kilometern)

### **Nutzerstatistiken**

- Nutzung nach Geschlecht (in Prozent)
- Nutzung nach Altersgruppe (in Prozent)

### **Top 10 Auswertungen**

- Meistgenutzte Fahrräder
- Beliebteste Startstationen
- Beliebteste Endstationen

### **Zeitbezogene Nutzung**

- Nutzung nach Zeitfenstern (in Prozent):
  - 00:00–06:00
  - 06:00–12:00
  - 12:00–18:00
  - 18:00–24:00

---

### **Screenshot: Dashboard**

![Screenshot](./data/ressourcenMarkdown/dashboard.png)

## 📂 Projektstruktur

Die Projektstruktur ist wie folgt organisiert:

```bash
├── airflow/
│   ├── dags/
│   ├── plugins/
│   ├── python/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── startup.sh
├── data/
│   ├── kaggle/
│   ├── output/
│   ├── raw/
│   └── Ressourcen.md
├── docker-compose.yml
├── README.md
└── requirements.txt
```

### 🔍 Erklärung der Struktur

- **`airflow/`**: Enthält alle relevanten Dateien und Verzeichnisse für Airflow:

  - **`dags/`**: Die DAGs (Directed Acyclic Graphs), welche die Workflow-Definitionen enthalten. Außerdem ist eine helper.py hinterlegt
  - **`plugins/`**: Individuelle Erweiterungen für Airflow.
  - **`python/`**: Skripte, die innerhalb der DAGs verwendet werden.
  - **`Dockerfile`**: Konfiguriert das Airflow-Docker-Image.
  - **`requirements.txt`**: Bibliotheken die in das Dockerimage geladen werden
  - **`startup.sh`**: Ein Skript, um den Airflow-Scheduler und den Webserver zu starten.

- **`data/`**: Verzeichnis für die Daten:

  - **`kaggle/`**: JSON mit Key für die Kaqqle API.
  - **`output/`**: Ergebniss des Workflows, Excel Datei
  - **`ressourcenMarkdown/`**: Ressourcen für die Dokumentation in der Markdown

- **`docker-compose.yml`**: Eine Datei für die Orchestrierung der Docker-Container.

- **`README.md`**: Dokumentation des Projekts.

## 📂 **Airflow**

Der Kern der Automatisierung bildet der Airflow-DAG, der in `airflow/dags/hubway_data_pipeline.py` definiert ist.

![Screenshot](./data/ressourcenMarkdown/dag_visualisierung.png)

Die obige Abbildung zeigt die einzelnen Schritte des DAGs und deren Abhängigkeiten. Im Folgenden werden diese Schritte systematisch erklärt:

### **Ablauf des Workflows**

1. **Erstellen der Eingabe- und Ausgabeverzeichnisse:**

   - Die Tasks `create_import_dir` und `create_output_dir` erstellen Verzeichnisse für die Eingabe- und Ausgabedaten. Dafür wird der benutzerdefinierte Operator `create_directory_operator.py` verwendet.

2. **Leeren der Verzeichnisse:**

   - Die erzeugten Verzeichnisse werden mit den Tasks `clear_import_dir` und `clear_output_dir` geleert. Dadurch wird sichergestellt, dass alte Daten den Workflow nicht beeinflussen. Der Operator `clear_directory_operator.py` wird hierfür genutzt.

3. **Herunterladen der Daten:**

   - Die Task `download_hubway_data` lädt die Hubway-Daten im CSV-Format mithilfe der `Kaggle API` herunter. Ein speziell entwickelter Operator übernimmt die Authentifizierung und den Download.

4. **Erstellung einer Jahr-Monat-Liste:**

   - Die Task `get_year_months` analysiert die heruntergeladenen Dateien und extrahiert eine Liste von Jahr-Monats-Werten (z. B. „201601“). Es werden nur Dateien berücksichtigt, deren Namen mit einer sechsstelligen Zahl beginnen. Die Liste dient als Basis für die weiteren Verarbeitungsschritte.

5. **Erstellen von HDFS-Verzeichnissen:**

   - Auf Basis der Jahr-Monats-Liste werden Verzeichnisse auf HDFS erstellt:
     - `create_hdfs_raw_data_dir`: Für die Speicherung der Rohdaten.
     - `create_hdfs_final_data_dir`: Für die bereinigten Daten.
   - Der Operator `hdfs_mkdirs_file_operator.py` wird hierbei verwendet.

6. **Übertragen der Rohdaten in HDFS:**

   - Die Task `upload_raw_data` lädt die heruntergeladenen CSV-Dateien in die zuvor erstellten HDFS-Verzeichnisse hoch.

7. **Bereinigung der Daten:**

   - Die Task `clean_raw_data` verarbeitet die Rohdaten. Dabei werden nur relevante Informationen extrahiert und nicht benötigte Felder entfernt. Das Skript `clean_raw_data.py` enthält die Logik dieser Datenbereinigung.

8. **Erstellen von Verzeichnissen für KPIs:**

   - Die Task `create_hdfs_kpi_dir` legt ein HDFS-Verzeichnis für die Speicherung der KPIs an.

9. **Berechnung der KPIs:**

   - Die Task `calculate_kpis` berechnet wichtige Kennzahlen wie durchschnittliche Fahrtdauer, Fahrdistanz oder Nutzung pro Altersgruppe. Das Skript `calculate_kpis.py` führt die Berechnungen durch und speichert die Ergebnisse im HDFS.

10. **Erstellen der Excel-Dashboards:**
    - Die Task `create_excel` erstellt eine Excel-Datei mit separaten Dashboards für jeden Monat. Die Daten werden mithilfe des Skripts `create_excel.py` visualisiert.

## 🔍 **Funktionen**

Die Datenverarbeitung der Rohdaten bis hin zum Dashboard erfolgt über drei Hauptskripte, die im Verzeichnis `airflow/python` abgelegt sind.

### 1. **Skript: `clean_raw_data.py`**

Dieses Skript bereinigt die Daten im ersten Schritt und sorgt dafür, dass nur die notwendigen Daten für die weitere Verarbeitung erhalten bleiben.

#### Wichtige Funktionen:

- **`determine_timeslot(hour: int)`**:

  - Ordnet die gegebene Stunde (0–24) einem Timeslot zu:
    - 0: 00:00–06:00
    - 1: 06:00–12:00
    - 2: 12:00–18:00
    - 3: 18:00–24:00

- **`is_within_timeslot(start_time: datetime, end_time: datetime, time_slot: int)`**:

  - Prüft, ob die Start- und Endzeit einer Fahrt in einem bestimmten Timeslot liegen und weist die Fahrt diesem zu.

- **`haversine(s_lat, s_lon, e_lat, e_lon)`**:

  - Berechnet die kürzeste Entfernung zwischen zwei geografischen Koordinaten (Start und Ziel) mithilfe der Haversine-Formel.

- **`calculate_age(birth_year)`**:

  - Bestimmt das Alter eines Nutzers basierend auf seinem Geburtsjahr.

- **`determine_generation(birth_year)`**:
  - Ordnet Nutzer anhand ihres Geburtsjahrs einer Generation (z. B. „Millennials“) zu.

#### Main-Funktion:

In der `main`-Funktion werden die oben genannten Berechnungen für jede Fahrt durchgeführt. Anschließend werden irrelevante Spalten entfernt, um Speicherplatz zu sparen. Die bereinigten Daten werden in HDFS gespeichert.

---

### 2. **Skript: `calculate_kpis.py`**

Dieses Skript berechnet die KPIs (Key Performance Indicators) für jeden Monat.

#### Wichtige Funktionen:

- **`calculate_average_kpis(df)`**:

  - Berechnet Durchschnittswerte für:
    - Fahrtdauer (`trip_duration`)
    - Fahrdistanz (`station_distance`)

- **`calculate_gender_share(df)`**:

  - Bestimmt den Anteil der Geschlechter (Männer, Frauen, unbekannt) an den Fahrten in Prozent.

- **`calculate_top_n(df, column_name, rank, return_type="value")`**:

  - Liefert die Top-N-Werte einer Spalte, z. B. die 10 meistgenutzten Fahrräder.

- **`calculate_time_slot_percentage(df, slot)`**:

  - Berechnet den Anteil der Fahrten, die in einem bestimmten Timeslot stattfinden (z. B. 06:00–12:00).

- **`calculate_generation_percentage(df, generation_value)`**:
  - Berechnet den Anteil der Nutzer einer bestimmten Generation (z. B. Babyboomer).

#### Main-Funktion:

Die `main`-Funktion ruft diese Funktionen für jeden Monats-Jahr-Wert auf. Die berechneten KPIs werden in neue Spalten des DataFrames eingefügt und anschließend in einem HDFS-Verzeichnis gespeichert.

---

### 3. **Skript: `create_excel.py`**

Dieses Skript erstellt eine Excel-Datei mit Dashboards für jeden Monat.

#### Wichtige Schritte:

- Konvertiert die KPI-Daten aus HDFS in einzelne Excel-Sheets.
- Fügt visuelle Dashboards mit Diagrammen und Tabellen hinzu, um die KPIs pro Monat darzustellen.

#### Main-Funktion:

Die `main`-Funktion liest die berechneten KPIs für jeden Monat, schreibt diese in Excel-Sheets und erstellt für jeden Monat ein Dashboard.

---

### **Zusammenhang der Skripte**

1. **`clean_raw_data.py`**:

   - Bereitet die Rohdaten auf und speichert sie als bereinigte Daten in HDFS.

2. **`calculate_kpis.py`**:

   - Nutzt die bereinigten Daten, um wichtige Kennzahlen zu berechnen und in HDFS zu speichern.

3. **`create_excel.py`**:
   - Verarbeitet die gespeicherten KPIs und erstellt eine visuelle Darstellung in Form von Dashboards.

Die klare Trennung der Skripte gewährleistet eine modulare und nachvollziehbare Datenpipeline.

## ⚙️ **Ausführung**

1. **Voraussetzungen:**

   - Docker und Docker-Compose installiert

2. **Schritte zur Ausführung:**

```bash
git clone https://github.com/DanielD884/BigData.git
cd BigData

docker-compose up
```

### Hadoop Cluster starten

```bash
docker exec -it hadoop bash
sudo su hadoop
start-all.sh
```

### Probleme

Bei der Ausfürhung auf einer VM auf der Google Cloud Platform, kann es zu Problemen kommen, dass der Airflow keine Schreibrechte hat, um die erstelle Excelfile in dem lokalen Verzeichnis data/outout/ zu schreiben. Deshalb ist vorher die notwendigen Rechte zu vergeben.

```bash
sudo chmod 777 data/output/

```

### Airflow Dags starten

Nun kann der Airflow-Server unter lokale VM(http://localhost:8080/) oder über die Google Cloud (<externe-IP>:8080) aufgerufen werden. Hier kann der Dags gestartet werden.