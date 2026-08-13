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
[ Azure Databricks Cluster ] ──> [ Delta Lake (Fact Due Tables) ]
             │                         │
             ├── (PySpark Dedupe) <────┘ (Delinquency & Rate-Limit Check)
             │
             ├── (Window Partitioning & Chunking: 1M rows/batch)
             ▼
[ Azure Data Lake Storage (ADLS Gen2) ]
             │
             ▼
[ Asynchronous API Dispatcher ] ──> (Concurrent HTTP Requests)
             │                                   │
             ▼                                   ▼
[ Real-Time ADX Telemetry / Kusto ]    [ External REST API ]

## ✨ Key Technical Features

* **Distributed Stateful Deduplication:** Multi-level deduplication engine implemented in PySpark using `left_outer` joins and row-level filtering.
* **Business-Level Dedupe:** Limits triggers to max 1 per device per day per business unit.
* **Global-Level Dedupe:** Enforces a global cap of max 3 triggers per device across all business units per day.
* **Asynchronous High-Throughput API Dispatching:** Leverages Python's `asyncio` and `requests` to concurrently process and stream chunked CSV buffers to REST APIs, bypassing standard I/O bottlenecks.
* **Fault-Tolerant Retries & Dynamic Token Refresh:** Engineered with the `tenacity` library for exponential backoff. Automatically intercepts 401 Unauthorized HTTP responses to refresh OAuth tokens inline without dropping batch state.
* **Real-Time Observability & Monitoring:** Fully integrated with Azure Data Explorer (ADX/Kusto) to log transformation pipeline states, record counts, execution time windows, and API response metadata natively from Python data classes.
* **Optimized Spark Memory Management:** Utilizes explicit Spark DataFrame caching (`.cache()`, `.persist()`), window function optimizations (`row_number() over Window.partitionBy`), and targeted unpersisting (`.unpersist()`) to eliminate data skew and OOM errors during chunked batch generation.


