# Databricks notebook source
# DBTITLE 1,ADX read-write class
import pandas as pd    
from azure.kusto.data import KustoConnectionStringBuilder, KustoClient    
from azure.kusto.ingest import QueuedIngestClient, IngestionProperties, IngestionStatus    
from azure.kusto.data.helpers import dataframe_from_result_table    
from azure.kusto.data.data_format import DataFormat    
from azure.kusto.ingest.status import KustoIngestStatusQueues    
from typing import Dict    
  
class ADXReadWrite:
    def __init__(self, connection_string, ingestion_connection_string, authority_id, app_key, aad_app_id):    
        self._connection_string = connection_string  
        self._ingestion_connection_string = ingestion_connection_string  
        self._authority_id = authority_id  
        self._app_key = app_key  
        self._aad_app_id = aad_app_id   
        self._setup_client()    
    
    def _setup_client(self):    
        self.kcsb = self._get_connection_string_builder(self._ingestion_connection_string)    
        self.client = KustoClient(self.kcsb)    
        self.ingestion = QueuedIngestClient(self.kcsb)    
        self.queue_status = KustoIngestStatusQueues(self.ingestion)    
    
    def _get_connection_string_builder(self, connection_string):    
        return KustoConnectionStringBuilder.with_aad_application_key_authentication(    
            connection_string=connection_string,    
            aad_app_id=self._aad_app_id,    
            app_key=self._app_key,    
            authority_id=self._authority_id    
        )    
    
    def execute_query(self, query: str, database_name:str) -> Dict:    
        client_query = self._get_query_client(self._connection_string)
        result = client_query.execute(database_name, query)    
        return self._get_result_as_json(result)    
    
    def _get_query_client(self, connection_string):    
        kcsb_query = self._get_connection_string_builder(connection_string)    
        return KustoClient(kcsb_query)    
    
    def _get_result_as_json(self, result):    
        return dataframe_from_result_table(result.primary_results[0]).to_json()    
    
    def data_ingest(self, init_data:Dict, table_name:str, db_name:str) -> IngestionStatus:    
        response = self._ingest_data(init_data, table_name, db_name)   
        return response.status    
    
    def _ingest_data(self, init_data, table_name, db_name):    
        ingestion_properties = self._get_ingestion_properties(db_name, table_name)    
        return self.ingestion.ingest_from_dataframe(df=pd.DataFrame(init_data, index=[0]), ingestion_properties=ingestion_properties)    
    
    def _get_ingestion_properties(self, db_name, table_name):    
        return IngestionProperties(database=db_name, table=table_name, data_format=DataFormat.JSON)    

# COMMAND ----------

# DBTITLE 1,Raise error if notebook fails after all retries
def handle_notebook_results(results):    
    failed_notebooks = [nb for nb in results if nb[1] is not None and nb[1].startswith('ERROR:')]    
    if failed_notebooks:    
        raise Exception(f'One or more notebooks failed: {", ".join(nb[0] for nb in failed_notebooks)}')

# COMMAND ----------

# DBTITLE 1,Parallel execution function for notebooks
import concurrent.futures

def run_notebook(path, timeout, parameters={}):
    try:
        # Running the notebook with given parameters and timeout
        result = dbutils.notebook.run(path, timeout, parameters)
        return path, result, 0  # 0 retries needed
    except Exception as e:
        return path, f"ERROR: {str(e)}", 1  # 1 retry used, assuming only one attempt

def parallel_notebooks(notebooks, num_notebooks_in_parallel=1, max_retries=1):
    # Use ThreadPoolExecutor to run notebooks in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_notebooks_in_parallel) as executor:
        # Future objects for all notebooks
        future_to_notebook = {executor.submit(run_notebook_with_retry, nb['path'], nb['timeout'], nb.get('parameters', {}), max_retries): nb for nb in notebooks}
        results = []
        for future in concurrent.futures.as_completed(future_to_notebook):
            nb = future_to_notebook[future]
            try:
                # Collecting results
                results.append(future.result())
            except Exception as exc:
                # Handle possible exceptions
                results.append((nb['path'], f"ERROR: {str(exc)}", max_retries))
        return results

def run_notebook_with_retry(path, timeout, parameters, retries):
    for attempt in range(retries + 1):
        try:
            return run_notebook(path, timeout, parameters)
        except Exception as e:
            if attempt == retries:
                return path, f"ERROR: {str(e)}", attempt

# COMMAND ----------

# DBTITLE 1,File existence checker
def file_existence_checker(file_name:str) -> bool:
  try:
    dbutils.fs.ls(file_name)
    return True
  except:
    return False

# COMMAND ----------

# DBTITLE 1,Dedupe Logic Check
from pyspark.sql.dataframe import DataFrame as PySparkDataFrame
from pyspark.sql.functions import col, when
from pyspark.sql.types import StructType, StructField, StringType

# Helper function to load CSV files and return a DataFrame  
def load_csv(base_path: str, path_extension:str) -> PySparkDataFrame:
    schema = StructType([
        StructField("IMEI", StringType(), True),
        StructField("Notification_Code", StringType(), True)
    ])
    empty_df = spark.createDataFrame([], schema)    
    
    if file_existence_checker(base_path):
        return spark.read.format("csv").option("header", "true").load(base_path+path_extension)
    return empty_df

# Helper function to join and update FILTER_STATUS  
def join_and_update_status(base_df: PySparkDataFrame, join_df: PySparkDataFrame, join_col:str, alias_join_col: str, status_col: str, status_value: str, duplicate_status_value:str ,exclude_statuses: list) -> PySparkDataFrame:
    return base_df.join(
            join_df, join_col, "left_outer"
        ).select(
            base_df["IMEI"], base_df["Notification_Code"], base_df["FILTER_STATUS"], join_df[join_col].alias(alias_join_col)
        ).withColumn(
            "FILTER_STATUS", when(
                    (col(alias_join_col).isNull()) & (~col("FILTER_STATUS").isin(exclude_statuses)), status_value
                ).when(
                    (col(alias_join_col).isNotNull()) & (~col("FILTER_STATUS").isin(exclude_statuses)), duplicate_status_value
                ).otherwise(col("FILTER_STATUS"))
        ).drop(alias_join_col)
  
# Biz dedupe - we cannot send more than 1 notification/business  
def load_and_deduplicate_biz(staging_path, date, business_name, base_df: PySparkDataFrame) -> PySparkDataFrame:  
    biz_staging_path = f"{staging_path}/{date}/{business_name}"
    # applied distinct to avoid extra values while doing left-outer join in biz-dedupe
    biz_df = load_csv(biz_staging_path, "/*").select("IMEI").distinct() 
    return join_and_update_status(base_df=base_df, join_df=biz_df, join_col="IMEI", alias_join_col="biz_IMEI", status_col="FILTER_STATUS", status_value="UNIQUE_BIZ", duplicate_status_value="DUPLICATE_BIZ", exclude_statuses=["NULL","DELINQUENT"])

# Global dedupe - in a day we cannot send more than 3 notifications/IMEI irrespective of business  
def load_and_deduplicate_global(staging_path, date, base_df: PySparkDataFrame) -> PySparkDataFrame:  
    global_staging_path = f"{staging_path}/{date}"  
    global_df = load_csv(global_staging_path, "/*/*").groupBy("IMEI").count().filter(col("count") >= 3)
    return join_and_update_status(base_df=base_df, join_df=global_df, join_col="IMEI", alias_join_col="glob_IMEI", status_col="FILTER_STATUS", status_value="UNIQUE_GLOBAL", duplicate_status_value="DUPLICATE_GLOBAL", exclude_statuses=["NULL","DELINQUENT", "DUPLICATE_BIZ"])