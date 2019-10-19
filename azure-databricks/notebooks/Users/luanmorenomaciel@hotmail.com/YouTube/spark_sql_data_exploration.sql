-- Databricks notebook source
-- MAGIC %md
-- MAGIC 
-- MAGIC # Exploração de Dados com SparkSQL
-- MAGIC 
-- MAGIC Luan Moreno Medeiros Maciel  
-- MAGIC One Way Solution
-- MAGIC 
-- MAGIC github = https://github.com/OneWaySolution/azure-databricks

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC spark-SQL is a spark module for structured data processing.  
-- MAGIC blob storage = brzluanmoreno/stgfiles/

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC documentation of spark-sql, dataframes and dataset  
-- MAGIC https://spark.apache.org/docs/latest/sql-programming-guide.html  
-- MAGIC https://docs.databricks.com/spark/latest/spark-sql/index.html
-- MAGIC 
-- MAGIC csv data source for Apache Spark [Databricks]  
-- MAGIC https://github.com/databricks/spark-csv

-- COMMAND ----------

-- MAGIC %fs ls "dbfs:/mnt/brzluanmoreno"

-- COMMAND ----------

-- MAGIC %fs ls "dbfs:/mnt/brzluanmoreno/autos-data"

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC 
-- MAGIC //reading csv files
-- MAGIC val df_autos = spark.read
-- MAGIC     .format("csv")
-- MAGIC     .option("header","true")
-- MAGIC     .option("inferSchema", "true")
-- MAGIC     .load("dbfs:/mnt/brzluanmoreno/autos-data/")

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC 
-- MAGIC //writing in parquet format
-- MAGIC df_autos.write.format("parquet").save("dbfs:/mnt/brzluanmoreno/autos-data-parquet")

-- COMMAND ----------

--https://spark.apache.org/docs/2.3.0/api/java/org/apache/spark/sql/types/package-summary.html

DROP TABLE IF EXISTS stg_autos;

CREATE TABLE stg_autos
USING org.apache.spark.sql.parquet
OPTIONS
(
path "dbfs:/mnt/brzluanmoreno/autos-data-parquet/"
)

-- COMMAND ----------

ANALYZE TABLE stg_autos COMPUTE STATISTICS

-- COMMAND ----------

SELECT dateCrawled,
       seller,
       offerType,
       price,
       vehicleType,
       yearOfRegistration,
       gearbox,
       powerPS,
       model,
       fuelType,
       brand
FROM stg_autos
LIMIT 100;

-- COMMAND ----------

SELECT COUNT(*)
FROM stg_autos;

-- COMMAND ----------

DESCRIBE stg_autos;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### Data Insights using Spark-SQL

-- COMMAND ----------

SELECT model, COUNT(*) AS Q
FROM stg_autos
GROUP BY model
ORDER BY Q DESC
LIMIT 10;

-- COMMAND ----------

SELECT brand, model, COUNT(*) AS Q
FROM stg_autos
GROUP BY brand, model
ORDER BY Q DESC
LIMIT 15;

-- COMMAND ----------

EXPLAIN EXTENDED SELECT brand, model, COUNT(*) AS Q FROM dbo.stg_autos GROUP BY brand, model ORDER BY Q DESC LIMIT 15;

-- COMMAND ----------

SELECT brand,
       SUM(price) AS price
FROM stg_autos
GROUP BY brand
LIMIT 3;

-- COMMAND ----------

SELECT vehicleType,
       SUM(price) AS price
FROM stg_autos
GROUP BY vehicleType
LIMIT 3;

-- COMMAND ----------

SELECT MONTH(dateCrawled) AS month,
       YEAR(dateCrawled) AS year,
       DAY(dateCrawled) AS day,
       SUM(price) AS price,
       vehicleType,
       brand,
       COUNT(*) AS Q
FROM stg_autos
GROUP BY MONTH(dateCrawled), YEAR(dateCrawled), DAY(dateCrawled), vehicleType, brand
ORDER BY Q DESC

-- COMMAND ----------

