# Databricks notebook source
# DBTITLE 1,Start Time for Process
import datetime
from pytz import timezone
import requests  
import traceback
import pandas as pd
import json
from dataclasses import dataclass, asdict
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark.sql.dataframe import DataFrame as PySparkDataFrame
from pyspark.sql.types import StructType, StructField, StringType
from typing import Dict
from azure.storage.blob import BlobServiceClient
from pandas import DataFrame as PandasDataFrame
from io import StringIO
from azure.core.exceptions import AzureError, ClientAuthenticationError
from tenacity import retry, stop_after_attempt, wait_exponential, RetryCallState, retry_if_exception_type

start_time = datetime.datetime.now(datetime.timezone.utc).astimezone(timezone('Asia/Kolkata')).strftime('%Y-%m-%dT%H:%M:%S.%f')
print(start_time)

# COMMAND ----------

# MAGIC %run "./Credentials"

# COMMAND ----------

# MAGIC %run "./Functions"

# COMMAND ----------

# DBTITLE 1,Getting Init Widgets
staging_path_base_url = dbutils.widgets.get("StagingPathBaseURL").strip()
transaction_id = dbutils.widgets.get("PipelineTriggerID").strip()
file_name = dbutils.widgets.get("BlobFileName").strip()
business_name = file_name.split("_")[1]
date_today_ist = datetime.datetime.now(datetime.timezone.utc).astimezone(timezone('Asia/Kolkata')).strftime('%Y/%m/%d')
db_name = 'cdt_tagM_db'
staging_table_name = 'DataCStaging'
init_table_name = "DataCInit"
kpi_table_name = "DataCPerformance"

print(f"date today - {date_today_ist}, file_name- = {file_name}, business_name - {business_name}, transaction_id = {transaction_id}")

# COMMAND ----------

# DBTITLE 1,BLOB File Reading
def create_blob_service_client(account_url, sas_token) -> BlobServiceClient:
    return BlobServiceClient(account_url=account_url, credential=sas_token)

def retry_exception(exception:RetryCallState):
    return {"error": str(exception), "stack_trace":str(traceback.format_exc())} 

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=20), 
       retry=retry_if_exception_type(AzureError | ClientAuthenticationError), retry_error_callback=retry_exception)
def read_csv_blob(account_url, container_name, blob_name, sas_token) -> PandasDataFrame:
    blob_service_client = create_blob_service_client(account_url, sas_token)
    container_client = blob_service_client.get_container_client(container_name)
    blob_client = container_client.get_blob_client(blob_name)

    blob_data = blob_client.download_blob().readall()
    blob_str = StringIO(blob_data.decode('utf-8'))
    return spark.createDataFrame(pd.read_csv(blob_str, header=0))

account_url = "https://cdtsftpstr01.blob.core.windows.net/"
container_name ="cdtsftp"
blob_name = f"SFDC_OUT/XYZPLUSOUT/{file_name}"
sas_token = dbutils.secrets.get("CDT-KeyVault", "CDT-SAS-Key")

device_finance_base_df = read_csv_blob(account_url, container_name, blob_name, sas_token).withColumnRenamed("LOAN_OVERVIEW_APPL_ID", "APPL_ID").withColumnRenamed("LOAN_OVERVIEW_IMEI", "IMEI").selectExpr("cast(APPL_ID as bigint) as APPL_ID","cast(cast(IMEI as bigint) as string) as IMEI", "cast(Notification_Code as string) as Notification_Code").cache()

# COMMAND ----------

# DBTITLE 1,Non Delinquent Filter
fact_loan_due_detail = spark.read.format("delta").load("abfss://edwp@cdtadlsdluatapp2.dfs.core.windows.net/data/dbo/fact_loan_due_detail/piidata/current/").where((col('BUCKET') > 0) & (col('INSTALLMENT_OVERDUE') > 0)).selectExpr('APPL_ID', 'FIN_REFERENCE')

device_finance_base_df = device_finance_base_df.withColumn(
    "FILTER_STATUS",
    when(col("IMEI").isNull(), "NULL").otherwise("NOT_NULL")
)

delinquency_check_df = join_and_update_status(device_finance_base_df, fact_loan_due_detail, "APPL_ID", "fact_APPL_ID", "FILTER_STATUS", "NOT_DELINQUENT", "DELINQUENT", ["NULL"])

# Deduplicate based on today's business files that may have already ran
biz_dedupe_df = load_and_deduplicate_biz(staging_path_base_url, date_today_ist, business_name, base_df=delinquency_check_df)

# Global dedupe dataframe containing all the records from the blob with their filter status (filter status is denotes the stage where the particular row should have got removed)
global_dedupe = load_and_deduplicate_global(staging_path_base_url, date_today_ist, base_df=biz_dedupe_df)

# Final dataframe containing eligible records
non_delinquent_df = global_dedupe.where("FILTER_STATUS = 'UNIQUE_GLOBAL'").drop(col("FILTER_STATUS")).cache()

# COMMAND ----------

# DBTITLE 1,Transformations Logged in Serialized Dict
transformations_df = global_dedupe.groupBy("FILTER_STATUS").count().collect()
transformations_dict = {row['FILTER_STATUS']: row['count'] for row in transformations_df}
transformations_json = json.dumps(transformations_dict)
print(transformations_dict)

# COMMAND ----------

# DBTITLE 1,Saving Blob Filtered File to Staging
base_file_filtered_path = staging_path_base_url + f"/{date_today_ist}/{file_name}"
print(f"saving the staging data to - {base_file_filtered_path}")
global_dedupe.write.format("csv").option("header", "true").mode("overwrite").save(base_file_filtered_path)

# COMMAND ----------

# DBTITLE 1,ADX Initialisation
adx = ADXReadWrite(  
    connection_string='https://cdtkpiadx.centralindia.kusto.windows.net',  
    ingestion_connection_string='https://ingest-cdtkpiadx.centralindia.kusto.windows.net',  
    authority_id=tennantID,  
    app_key=aunthenticationKey,  
    aad_app_id=applicationID
)

# COMMAND ----------

# DBTITLE 1,Creating and Logging Initial Ingest Data in Python
@dataclass  
class IngestDataInit:
  CG_FILENAME: str
  TRANSACTION_ID: str
  CG_COUNT: int
  TRANSFORMATIONS: str
  ELIGIBLE_COUNT: int

init_data = IngestDataInit( 
              CG_FILENAME=file_name,
              TRANSACTION_ID=transaction_id,
              CG_COUNT=device_finance_base_df.count(),
              TRANSFORMATIONS=transformations_json,             
              ELIGIBLE_COUNT=non_delinquent_df.count()
            ) 

init_adx_response = adx.data_ingest(asdict(init_data), init_table_name, db_name)
print(f"Logging Status for Init Data Log: {init_adx_response.value}")

# COMMAND ----------

# DBTITLE 1,Helper Functions
def generate_filename() -> str:
    current_datetime = datetime.datetime.now()
    formatted_datetime = current_datetime.strftime('%Y%m%d%H%M%S')
    filename = f"DCNOTIF_{formatted_datetime}_REQ.csv"    
    return filename

def validate_inputs(filename: str, data):
    if not isinstance(filename, str) or not filename:
        raise ValueError("Invalid or empty filename.")
    if not isinstance(data, PySparkDataFrame) or data.isEmpty():
        raise ValueError("Invalid or empty PySpark DataFrame.")

def save_to_staging(filename: str, data: PySparkDataFrame):
    validate_inputs(filename, data)
    adls_path = f"/{date_today_ist}/{business_name}/{filename}"
    data.write.format("csv").option("header", "true").mode("overwrite").save(staging_path_base_url + adls_path)
    return adls_path

# COMMAND ----------

# DBTITLE 1,Saving Batch To Staging
# TODO - create a partitionBy code in window operation to avoid putting all data in a single partition. I have partitioned by APPL_ID for now. Will check for data skewness with respect to it.
batch_count_list:list[int]=[]
batches_filenames_list:list[str] = []

def batch_save_to_staging(df: PySparkDataFrame, step: int):
    # Assign a unique monotonically increasing ID to each row
    df_with_id = df.withColumn("row_id", monotonically_increasing_id())
    window = Window.partitionBy("Notification_Code").orderBy("row_id")
    df_window_partition = df_with_id.withColumn("row", row_number().over(window)).persist()

    # Write each batch to staging path with 10,00,000
    for i in range(1, df.count()+1, step):
      batch_df = df_window_partition.filter((col("row") >= i) & (col("row") < i + step))
      batch_df = batch_df.withColumn("TriggerID", expr("uuid()")).drop("row_id", "row").persist()
      batch_count_list.append(batch_df.count())
      adls_batch_staging_path = save_to_staging(filename=generate_filename(), data=batch_df)
      print(f"batch {i} staging path - {adls_batch_staging_path}")
      batches_filenames_list.append(adls_batch_staging_path)
      batch_df.unpersist()

    df_window_partition.unpersist()

batch_size = 10_00_000
batch_save_to_staging(non_delinquent_df, batch_size)

# COMMAND ----------

# DBTITLE 1,Data Staging Process for Batch File Ingestion
@dataclass  
class IngestDataStaging:
  TRANSACTION_ID: str
  CG_FILENAME: str
  DATA_C_FILENAME: str
  BATCH_FILE_COUNT: int

for i, batch_filename in enumerate(batches_filenames_list):
  staging_data = IngestDataStaging(
                TRANSACTION_ID=transaction_id,  
                CG_FILENAME=file_name,
                DATA_C_FILENAME=batch_filename,
                BATCH_FILE_COUNT=batch_count_list[i]
              )
   
  staging_adx_response = adx.data_ingest(asdict(staging_data), staging_table_name, db_name)
  print(f"Logging Status for {batch_filename}: {staging_adx_response.value}")

# COMMAND ----------

# DBTITLE 1,End Time for Process
end_time = datetime.datetime.now(datetime.timezone.utc).astimezone(timezone('Asia/Kolkata')).strftime('%Y-%m-%dT%H:%M:%S.%f')
print(end_time)

# COMMAND ----------

# DBTITLE 1,KPI Logging for Main Notebook
@dataclass  
class IngestNotebookKPI:
  TRANSACTION_ID: str
  CG_FILENAME: str
  ITEM_TYPE: str
  START_TIME: str
  END_TIME: str

kpi_data = IngestNotebookKPI( 
  TRANSACTION_ID = transaction_id,
  CG_FILENAME = file_name,
  ITEM_TYPE = "Primary",
  START_TIME = start_time,
  END_TIME = end_time
) 

kpi_adx_response = adx.data_ingest(asdict(kpi_data), kpi_table_name, db_name)
print(f"Logging Status for KPI Data Log: {kpi_adx_response.value}")