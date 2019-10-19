-- Databricks notebook source
-- MAGIC %md
-- MAGIC 
-- MAGIC # Databricks File System [DBFS]
-- MAGIC 
-- MAGIC Luan Moreno Medeiros Maciel  
-- MAGIC One Way Solution
-- MAGIC 
-- MAGIC github = https://github.com/OneWaySolution/azure-databricks

-- COMMAND ----------

-- MAGIC %fs ls

-- COMMAND ----------

-- MAGIC %fs head "dbfs:/mnt/brzluanmoreno/autos-data/autos_0.csv"

-- COMMAND ----------

-- MAGIC %fs ls "dbfs:/databricks-datasets/"

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC 
-- MAGIC mkdirs "dbfs:/new-data-sql"

-- COMMAND ----------

-- MAGIC %fs 
-- MAGIC 
-- MAGIC ls "dbfs:/mnt/brzluanmoreno/"

-- COMMAND ----------

