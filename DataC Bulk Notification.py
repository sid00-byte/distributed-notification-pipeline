# Databricks notebook source
# DBTITLE 1,Basic Library Initialisation
import asyncio
import requests  
import traceback
import json
import datetime
from tenacity import RetryError
from pytz import timezone
from io import StringIO
from dataclasses import dataclass, asdict
from tenacity import retry, stop_after_attempt, wait_exponential, RetryCallState

start_time = datetime.datetime.now(datetime.timezone.utc).astimezone(timezone('Asia/Kolkata')).strftime('%Y-%m-%dT%H:%M:%S.%f')
print(start_time)

# COMMAND ----------

# MAGIC %run "./Credentials"

# COMMAND ----------

# MAGIC %run "./Functions"

# COMMAND ----------

# DBTITLE 1,Getting Init Widgets
adx = ADXReadWrite(  
    connection_string='https://cdtkpiadx.centralindia.kusto.windows.net',  
    ingestion_connection_string='https://ingest-cdtkpiadx.centralindia.kusto.windows.net',  
    authority_id=tennantID,  
    app_key=aunthenticationKey,  
    aad_app_id=applicationID
)

transaction_id = dbutils.widgets.get("PipelineTriggerID").strip()
staging_path_base_url = dbutils.widgets.get("StagingPathBaseURL").strip()
file_name = dbutils.widgets.get("BlobFileName").strip()
adx_read_response = adx.execute_query(query=f"dataCBulkAPI('{transaction_id}')", database_name="cdt_tagM_db")
filenames_list = json.loads(adx_read_response)["DATA_C_FILENAME"].values()
staging_table_name = 'DataCLogs'  
db_name = 'cdt_tagM_db'
kpi_table_name = "DataCPerformance"

print(f"Filename - {filenames_list}")

# COMMAND ----------

# DBTITLE 1,Get Token Access
def retry_exception(exception:RetryCallState):
    print({"error": str(exception), "stack_trace":str(traceback.format_exc())})
    raise exception
  
@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=2, max=20), retry_error_callback=retry_exception)  
def get_token(token_api:str="https://api.datac.cloud/api/token/", **kwargs):
    headers = {
        'Content-Type': 'application/json'  
    }
    payload = {  
        "username": "john.doe@xyzcorp.in",  
        "password": "cdt#John@123"  
    }
    response = requests.post(token_api, headers=headers, json=payload)  
    response.raise_for_status()
    
    return response.json()

datac_access_token = get_token()["access"]

# COMMAND ----------

# DBTITLE 1,Bulk Notification Function
@dataclass
class BulkNotificationResponse:
    status:str
    status_code:str
    datacultr_api_response:str
    filename:str

def read_file_from_adls_to_buffer(filename: str) -> bytes:
    data_df = spark.read.format("csv").option("header", "true").load(staging_path_base_url + filename)
    pandas_df = data_df.toPandas()
    csv_buffer = StringIO()
    pandas_df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)  # rewind to start of the StringIO object to read its content
    
    return csv_buffer.getvalue()

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=20))  
async def send_file_to_api(datacult_access_token: str, transaction_id: str, filename: str, bulk_notification_url:str="https://api.datac.cloud/api/v1/lcycle/dem_xyz/applynotificationbulk/",**kwargs) -> BulkNotificationResponse:  
    try:  
        file_contents = read_file_from_adls_to_buffer(filename)
        
        payload = {'TransactionId': transaction_id}
          
        headers = {  
            'Authorization': f'Bearer {datac_access_token}'  
        }  
        files = [
          ('file',(filename,file_contents,'text/csv'))
        ]          
        response = requests.post(  
            bulk_notification_url,  
            headers=headers, 
            data=payload, 
            files=files  
        )
          
        if response.status_code in range(200, 400):
            return BulkNotificationResponse(status="success", status_code=str(response.status_code), datac_api_response=response.text, filename=filename)  
        else:  
            response.raise_for_status()  # Will raise HTTPError if status code is not 200-399
    except requests.exceptions.HTTPError as e:  
        if e.response.status_code == 401:  
            # Handle the authentication error: refresh the token and retry  
            print("Authentication failed. Refreshing token...")  
            datac_access_token = get_token()["access"]  
            return send_file_to_api(datac_access_token, transaction_id, filename, bulk_notification_url)  
        else:  
            return BulkNotificationResponse(status="error", status_code=str(e.response.status_code), datac_api_response=str(e), filename=filename)

# COMMAND ----------

# DBTITLE 1,Concurrent API calls
tasks = []
for filename in filenames_list:
  tasks.append(send_file_to_api(datac_access_token=datac_access_token, transaction_id=transaction_id, filename=filename.strip()))

concurrent_datac_responses = await asyncio.gather(*tasks, return_exceptions=True)

# COMMAND ----------

# DBTITLE 1,Concurrent API Response Check
for response in concurrent_datac_responses:
    if isinstance(response, RetryError):
        print(f"Task failed after retries with exception: {response.last_attempt.exception()}")
    else:
        print(f"Task succeeded with response: {response}")

# COMMAND ----------

# DBTITLE 1,ADX logging
@dataclass  
class IngestDataFinal:  
    TRANSACTION_ID: str    
    DATA_C_FILENAME: str
    DATA_C_RESPONSE: str

for datac_response in concurrent_datac_responses:
  staging_data_log = IngestDataFinal(
      TRANSACTION_ID=transaction_id,  
      DATA_C_FILENAME=datac_response.filename,      
      DATA_C_RESPONSE=f"""{{"status": "{datac_response.status}", "status_code": "{datac_response.status_code}", "datac_api_response": {datac_response.datac_api_response}}}"""
  )

  adx_response = adx.data_ingest(asdict(staging_data_log), staging_table_name, db_name)

  print(f"Logging Status for {datac_response.filename}: {adx_response.value}")

# COMMAND ----------

# DBTITLE 1,End Time for Process
end_time = datetime.datetime.now(datetime.timezone.utc).astimezone(timezone('Asia/Kolkata')).strftime('%Y-%m-%dT%H:%M:%S.%f')
print(end_time)

# COMMAND ----------

# DBTITLE 1,KPI Logging for Notification Notebook
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
  ITEM_TYPE = "BulkNotification",
  START_TIME = start_time,
  END_TIME = end_time
) 

kpi_adx_response = adx.data_ingest(asdict(kpi_data), kpi_table_name, db_name)
print(f"Logging Status for KPI Data Log: {kpi_adx_response.value}")