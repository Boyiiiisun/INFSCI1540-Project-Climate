# Climate Data Warehouse Project

This project was developed for **INFSCI 1540: Data Engineering** at the **University of Pittsburgh**.

Contributor: Boyi Sun and Murphy Zhao

## Project Overview
The goal of this project is to design and implement a **data warehouse** based on **climate data** from **NOAA Climate Data Online**. The system supports data ingestion, data preparation, warehouse design, and analytical queries for climate trend analysis.

## Data Source
NOAA Climate Data Online  
http://www.ncdc.noaa.gov/cdo-web/

## Main Components
- Docker
- Kafka
- ksqlDB
- MySQL
- Python

## Project Features
- Climate data ingestion through Kafka streams
- Data preparation and transformation
- STAR schema design
- Pre-aggregated summary tables
- OLAP queries for climate analysis


## 🚀 Operation Guide

Follow these steps to set up the environment and run the data pipeline.

### Step 1: Start the Environment
Navigate to your project directory and start the Docker containers:
```powershell
cd ~/documents/code/docker/climate
docker compose up -d
````

### Step 2: Initialize Databases

Run the preparation scripts to create the **ODB** (Operational Database) and **DW** (Data Warehouse) schemas:

```powershell
python prepare_odb.py
python prepare_dw.py
```

### Step 3: Load Initial Data

Populate the initial monthly summary into the system:

```powershell
python load_monthly_summary.py
```

### Step 4: Start Kafka Consumer

Open a new terminal window and start the consumer to listen for incoming messages:

```powershell
python consumer.py
```

### Step 5: Configure ksqldb Streams and Tables

Connect to the ksqldb CLI and execute the stream processing logic:

1.  **Access ksqldb-cli**:
    ```powershell
    docker exec -it ksqldb-cli ksql http://ksqldb-server:8088
    ```
2.  **Execute Scripts**: Copy and paste the content from the following files into the ksql terminal in order:
      * `create_raw_stream.sql`
      * `create_clean_stream.sql`
      * `create_monthly_summary.sql`

### Step 6: Trigger Data Pipeline

In separate terminal windows, run the producer and the final load script to process daily data:

```powershell
python dly_producer.py
python load.py
```

-----

## 📊 Database Monitoring

You can monitor the data updates in real-time via the following web interfaces:

  * **ODB (Operational DB)**: [http://localhost:15000](https://www.google.com/search?q=http://localhost:15000)
  * **DW (Data Warehouse)**: [http://localhost:25000](https://www.google.com/search?q=http://localhost:25000)

