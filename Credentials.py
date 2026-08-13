# Databricks notebook source
tennantID = dbutils.secrets.get("CDT-KeyVault", "CDT-TENANT")
aunthenticationKey = dbutils.secrets.get("CDT-KeyVault", "PERS-APP-SECRET")
applicationID = dbutils.secrets.get("CDT-KeyVault", "PERS-APP-ID")

# COMMAND ----------

endpoint = "https://login.microsoft.com/"+ tennantID + "/oauth2/token"

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.cdtdlstandardisedzone.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.cdtdlstandardisedzone.dfs.core.windows.net",
               "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.cdtdlstandardisedzone.dfs.core.windows.net", applicationID)
spark.conf.set("fs.azure.account.oauth2.client.secret.cdtdlstandardisedzone.dfs.core.windows.net", aunthenticationKey)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.cdtdlstandardisedzone.dfs.core.windows.net", endpoint)

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.cdtadlsdluatapp2.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.cdtadlsdluatapp2.dfs.core.windows.net",
               "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.cdtadlsdluatapp2.dfs.core.windows.net", applicationID)
spark.conf.set("fs.azure.account.oauth2.client.secret.cdtadlsdluatapp2.dfs.core.windows.net", aunthenticationKey)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.cdtadlsdluatapp2.dfs.core.windows.net", endpoint)

# COMMAND ----------

spark.conf.set("spark.sql.avro.compression.codec", "deflate")
spark.conf.set("spark.sql.avro.deflate.level", "5")

# COMMAND ----------

spark.conf.set("fs.azure.account.key.cdtdlstandardisedzone.dfs.core.windows.net", aunthenticationKey)