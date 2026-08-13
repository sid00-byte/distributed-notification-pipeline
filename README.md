# Distributed Data Ingestion & Async Batch Dispatcher 🚀

![Python](https://img.shields.io/badge/Python-3.10-blue?logo=python&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache_Spark-3.4.1-orange?logo=apachespark&logoColor=white)
![Databricks](https://img.shields.io/badge/Databricks-Runtime_13.3-FF3621?logo=databricks&logoColor=white)
![Azure](https://img.shields.io/badge/Azure-Data_Lake_|_ADX-0078D4?logo=microsoftazure&logoColor=white)

An enterprise-grade, distributed data engineering pipeline designed to ingest, process, and dispatch millions of records asynchronously. Built natively on **Azure Databricks** and **PySpark**, this platform executes complex stateful deduplication against **Delta Lake** tables, partitions data into optimized staging batches, and streams concurrent payloads to third-party REST APIs.

---

## 🏗️ Architecture Overview

The pipeline is split into two primary orchestration layers: **Data Processing (PySpark)** and **Event Dispatching (Asyncio)**.

```text
[ Azure Blob / SFTP ]
             │ (Exponential Backoff Ingestion via Azure SDK)
             ▼
[ Azure Databricks Cluster ] <── [ Delta Lake (Fact Due Tables) ]
             │                         (Delinquency Reference Data)
             ├── (PySpark Deduplication & Rate-Limit Check)
             │
             ├── (Window Partitioning & Chunking: 1M rows/batch)
             │
             ├──> [ Real-Time ADX Telemetry (Init & Staging Logs) ]
             ▼
[ Azure Data Lake Storage (ADLS Gen2) ]
             │
             ▼
[ Asynchronous API Dispatcher ] ──> (Concurrent HTTP Requests) ──> [ External REST API ]
             │
             ▼
[ Real-Time ADX Telemetry (API Response & KPI Logs) ]

