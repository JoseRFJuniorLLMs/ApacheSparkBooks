# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ### Azure Databricks + Azure Blob Storage
# MAGIC https://docs.databricks.com/spark/latest/data-sources/azure/azure-storage.html

# COMMAND ----------

# MAGIC %md
# MAGIC https://docs.azuredatabricks.net/user-guide/secrets/secret-scopes.html  
# MAGIC https://docs.azuredatabricks.net/user-guide/dev-tools/databricks-cli.html
# MAGIC 
# MAGIC pip install pip install databricks-cli
# MAGIC <br>
# MAGIC databricks secrets create-scope --scope az-bs-abfsowshq 
# MAGIC <br>
# MAGIC databricks secrets list-scopes 
# MAGIC <br>
# MAGIC databricks secrets put --scope az-bs-abfsowshq --key key-abfsowshq [Wz0kq70Su/n+aD0ifcjCjft0moSLeOJE+aXRs5rIRL34olYNziNke/svUOIayx0p8oQV6+UZ/yhMvx/r6vcHoQ==]
# MAGIC <br>
# MAGIC databricks secrets list --scope az-bs-abfsowshq

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC configuring credentials to access **azure blob storage** account for **bs-stg-files** and **bs-production-yelp** those buckets contains files that are going to be used for our ingestion pipeline process on azure databricks, configuring the buckets to appear on the **databricks file system [DBFS]**
# MAGIC 
# MAGIC persistence layer between azure databricks and azure blob storage - https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC dbutils.fs.mount(
# MAGIC   source = "wasbs://bs-stg-files@abfsowshq.blob.core.windows.net",
# MAGIC   mount_point = "/mnt/stg-files",
# MAGIC   extra_configs = {"fs.azure.account.key.abfsowshq.blob.core.windows.net":dbutils.secrets.get(scope = "az-bs-abfsowshq", key = "key-abfsowshq")})

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC dbutils.fs.mount(
# MAGIC   source = "wasbs://bs-production-yelp@abfsowshq.blob.core.windows.net",
# MAGIC   mount_point = "/mnt/prod-files",
# MAGIC   extra_configs = {"fs.azure.account.key.abfsowshq.blob.core.windows.net":dbutils.secrets.get(scope = "az-bs-abfsowshq", key = "key-abfsowshq")})

# COMMAND ----------

display(dbutils.fs.ls("/mnt/stg-files"))

# COMMAND ----------

display(dbutils.fs.ls("/mnt/prod-files"))

# COMMAND ----------

dbutils.fs.unmount("/mnt/stg-files")
dbutils.fs.unmount("/mnt/prod-files")