# IoT Traffic Monitoring Solution

## 📖 Project Overview

This project implements a **Cloud-Native, Event-Driven Architecture** designed to process, summarize, and consolidate IoT traffic data from distributed sources.

The goal is to analyze network traffic (Flow Duration and Packet Counts) between Source and Destination IPs to detect anomalies. Instead of using expensive, always-on databases, this solution leverages **Amazon S3** as a state store and **Amazon SQS** for orchestration, complying with requirements for temporary infrastructure and resilience.

### Architecture Flow
1. **Ingestion:** Raw CSV files are uploaded to an S3 Bucket.
2. **Trigger:** S3 Event Notifications push messages to a Standard SQS Queue.
3. **Summarization:** A worker aggregates the raw data per day and pushes an intermediate JSON summary to S3.
4. **Consolidation:** A second worker (triggered via a FIFO Queue) updates the historical running statistics (Count, Sum, Sum of Squares) using Welford's Algorithm logic.
5. **Reporting:** A client tool generates a final CSV report with Average and Standard Deviation calculations.

---

## 🏗️ Components & Class Responsibilities

The project is divided into three Maven modules. Here is the breakdown of each class:

### 1. Module: `iot-ingestion` (Upstream)

* **`UploadClient`**
    * **Role:** Simulates an IoT branch uploading data.
    * **Action:** Takes a local CSV file path as input and uploads it to the **Raw Data S3 Bucket**. It uses a UUID to ensure unique filenames.

* **`SummarizeWorker`**
    * **Role:** The first processing unit (stateless).
    * **Action:** 1. Long-polls the `queue-summarize` (Standard Queue).
        2. Downloads the raw CSV from S3.
        3. Aggregates traffic data by `SrcIP:DstIP:Date` (sums duration and packets).
        4. Writes a lightweight JSON summary to the **Interim S3 Bucket**.
        5. Sends a notification message to the `queue-consolidate.fifo`, using the Source IP as the Message Group ID to ensure sequential processing.

### 2. Module: `iot-analytics` (Downstream)

* **`ConsolidatorWorker`**
    * **Role:** The core analytics unit (stateful).
    * **Action:**
        1. Polls the `queue-consolidate.fifo` (First-In-First-Out).
        2. Downloads the intermediate JSON summary.
        3. Loads the current historical state (if any) from the **State S3 Bucket** for that specific IP pair.
        4. Updates the statistics (N, Sum, SumSquares) using atomic read-modify-write logic.
        5. Saves the new state back to S3 and deletes the intermediate file (cleanup).

* **`ExportClient`**
    * **Role:** Reporting tool.
    * **Action:** Scans the **State S3 Bucket**, applies statistical formulas (Variance = $E[X^2] - (E[X])^2$) to calculate the Average and Standard Deviation, and generates a `report.csv` file.

### 3. Module: `iot-shared`

* **`TrafficState` (Record):** Data model representing the persistent historical state (stores sums and squares, not raw data).
* **`IntermediateSummary` (Record):** Data model for the daily summary passed between workers.
* **`JsonUtils`:** Utility class for efficient Jackson JSON serialization/deserialization.

---

## ⚙️ AWS Configuration Guide

To run this project, you must set up the specific AWS resources and configure your local environment.

### 1. Infrastructure Setup (AWS Console)

* **S3 Buckets:** Create 3 buckets (ensure names are unique globally):
    1.  `iot-raw-grp13-1` (For raw CSVs) -> **Configure Event Notification** to send to `queue-summarize`.
    2.  `iot-interim-grp13-1` (For temporary JSONs).
    3.  `iot-state-grp13-1` (For historical JSON states).

* **SQS Queues:** Create 2 queues:
    1.  `queue-summarize` (Standard Queue).
    2.  `queue-consolidate.fifo` (FIFO Queue). **Important:** Enable "Content-Based Deduplication".

### 2. Application Configuration
Open the Java files and update the `CONFIGURATION` constants with your specific resource names if they differ:
* `SummarizeWorker.java`: Update `QUEUE_URL` and bucket names.
* `ConsolidatorWorker.java`: Update `QUEUE_URL` (FIFO) and bucket names.
* `UploadClient.java`: Update `BUCKET_NAME`.

### 3. Local Credentials Setup
If running outside of an EC2 instance (e.g., local laptop with AWS Academy), you must export your temporary credentials as environment variables before running the JARs.

**Linux/Mac:**
```bash
export AWS_ACCESS_KEY_ID="ASIA..."
export AWS_SECRET_ACCESS_KEY="wJalr..."
export AWS_SESSION_TOKEN="IQoJb..."
export AWS_REGION="us-east-1"
```
## ▶️ Execution Steps

### 1. Build the Project
```bash
mvn clean package
```

### 2. Start Workers (in separate terminals):
```bash
# Terminal 1: Analytics
java -jar iot-analytics/target/iot-analytics-1.1.jar

# Terminal 2: Ingestion
java -jar iot-ingestion/target/iot-ingestion-1.1.jar
```

### 3. Inject Data:
```bash
# Terminal 3
java -cp iot-ingestion/target/iot-ingestion-1.1.jar com.iot.ingestion.UploadClient data/dataset.csv
```

### 4. Generate Report:
```bash
java -cp iot-analytics/target/iot-analytics-1.1.jar com.iot.analytics.ExportClient
```
