# Databricks notebook source
# MAGIC %md
# MAGIC Azure SQL Dw
# MAGIC ===

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC showing tables created and persisted on azure databricks
# MAGIC ---

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC show tables;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Creating the following views to perform the date insertion on **azure sql dw**:
# MAGIC 
# MAGIC + dim_users
# MAGIC + dim_business
# MAGIC + dim_date
# MAGIC + ft_review

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Creating Views 

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP VIEW IF EXISTS dim_users;
# MAGIC 
# MAGIC CREATE VIEW dim_users
# MAGIC AS
# MAGIC SELECT user_id AS nk,
# MAGIC        name,
# MAGIC        average_stars AS stars,
# MAGIC        fans,
# MAGIC        review_count AS review,
# MAGIC        useful,
# MAGIC        yelping_since AS since
# MAGIC FROM stg_users

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP VIEW IF EXISTS dim_business;
# MAGIC 
# MAGIC CREATE VIEW dim_business
# MAGIC AS
# MAGIC SELECT business_id AS nk,
# MAGIC        name,
# MAGIC        city,
# MAGIC        state,
# MAGIC        category,
# MAGIC        review_count AS review,
# MAGIC        stars
# MAGIC FROM stg_business

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP VIEW IF EXISTS dim_date;
# MAGIC 
# MAGIC CREATE VIEW dim_date
# MAGIC AS
# MAGIC SELECT *
# MAGIC FROM stg_date

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP VIEW IF EXISTS fact_review;
# MAGIC 
# MAGIC CREATE VIEW fact_review
# MAGIC AS
# MAGIC SELECT review_id,
# MAGIC        business_id,
# MAGIC        user_id,
# MAGIC        stars,
# MAGIC        useful,
# MAGIC        1 AS amount,
# MAGIC        date
# MAGIC FROM ft_review

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Selecting Created Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM dim_users
# MAGIC LIMIT 100;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM dim_business
# MAGIC LIMIT 100;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM dim_date
# MAGIC LIMIT 100;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT COUNT(*)
# MAGIC FROM fact_review
# MAGIC LIMIT 100;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE IF EXISTS fact_insert;
# MAGIC 
# MAGIC CREATE TABLE fact_insert
# MAGIC AS
# MAGIC SELECT fr.review_id AS nk_review,
# MAGIC        du.nk AS nk_users,
# MAGIC        db.nk AS nk_business,
# MAGIC        dd.date AS nk_date,
# MAGIC        fr.stars AS review_stars,
# MAGIC        fr.useful AS review_useful,
# MAGIC        fr.amount AS review_amount       
# MAGIC FROM fact_review AS fr
# MAGIC LEFT OUTER JOIN dim_users AS du
# MAGIC ON fr.user_id = du.nk
# MAGIC LEFT OUTER JOIN dim_business AS db
# MAGIC ON fr.business_id = db.nk
# MAGIC LEFT OUTER JOIN dim_date AS dd
# MAGIC ON fr.date = dd.date

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ## transforming spark-sql to dataframe

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC val fact_insert = spark.table("fact_insert")
# MAGIC display(fact_insert)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## azure sql dw [scala code]

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC val blobStorage = "abfsowshq.blob.core.windows.net"
# MAGIC val blobContainer = "sqldw"
# MAGIC val blobAccessKey =  "Wz0kq70Su/n+aD0ifcjCjft0moSLeOJE+aXRs5rIRL34olYNziNke/svUOIayx0p8oQV6+UZ/yhMvx/r6vcHoQ=="

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC val tempDir = "wasbs://" + blobContainer + "@" + blobStorage +"/tempDirs"

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC val acntInfo = "fs.azure.account.key."+ blobStorage
# MAGIC sc.hadoopConfiguration.set(acntInfo, blobAccessKey)

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC  val dwDatabase = "OwsHQDw"
# MAGIC  val dwServer = "owshqsqldw.database.windows.net" 
# MAGIC  val dwUser = "luanmoreno"
# MAGIC  val dwPass = "qq11ww22!!@@"
# MAGIC  val dwJdbcPort =  "1433"
# MAGIC  val dwJdbcExtraOptions = "encrypt=true;trustServerCertificate=true;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
# MAGIC  val sqlDwUrl = "jdbc:sqlserver://" + dwServer + ".database.windows.net:" + dwJdbcPort + ";database=" + dwDatabase + ";user=" + dwUser+";password=" + dwPass + ";$dwJdbcExtraOptions"
# MAGIC  val sqlDwUrlSmall = "jdbc:sqlserver://" + dwServer + ".database.windows.net:" + dwJdbcPort + ";database=" + dwDatabase + ";user=" + dwUser+";password=" + dwPass

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC spark.conf.set(
# MAGIC    "spark.sql.parquet.writeLegacyFormat",
# MAGIC    "true")

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC fact_insert.write.format("com.databricks.spark.sqldw").option("url", sqlDwUrlSmall).option("dbtable", "ft_review").option( "forward_spark_azure_storage_credentials","True").option("tempdir", tempDir).mode("overwrite").save()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## azure sql dw [spark-sql code]

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC - dropping tables in azure sql dw
# MAGIC 
# MAGIC DROP TABLE ft_reviews;  
# MAGIC DROP TABLE dm_users;  
# MAGIC DROP TABLE dm_business;  
# MAGIC DROP TABLE dm_date;  

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE TABLE ft_reviews_1
# MAGIC USING com.databricks.spark.sqldw
# MAGIC OPTIONS (
# MAGIC   url 'jdbc:sqlserver://owshqsqldw.database.windows.net:1433;database=OwsHQDw;user=luanmoreno@owshqsqldw;password={qq11ww22!!@@};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;',
# MAGIC   forwardSparkAzureStorageCredentials 'true',
# MAGIC   dbTable 'ft_reviews_1',
# MAGIC   tempDir 'wasbs://sqldw@abfsowshq.blob.core.windows.net/tempDirs'
# MAGIC )
# MAGIC AS SELECT * FROM fact_insert;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE TABLE dm_users
# MAGIC USING com.databricks.spark.sqldw
# MAGIC OPTIONS (
# MAGIC   url 'jdbc:sqlserver://owshqsqldw.database.windows.net:1433;database=OwsHQDw;user=luanmoreno@owshqsqldw;password={qq11ww22!!@@};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;',
# MAGIC   forwardSparkAzureStorageCredentials 'true',
# MAGIC   dbTable 'dm_users',
# MAGIC   tempDir 'wasbs://sqldw@abfsowshq.blob.core.windows.net/tempDirs'
# MAGIC )
# MAGIC AS SELECT * FROM dim_users;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE TABLE dm_business
# MAGIC USING com.databricks.spark.sqldw
# MAGIC OPTIONS (
# MAGIC   url 'jdbc:sqlserver://owshqsqldw.database.windows.net:1433;database=OwsHQDw;user=luanmoreno@owshqsqldw;password={qq11ww22!!@@};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;',
# MAGIC   forwardSparkAzureStorageCredentials 'true',
# MAGIC   dbTable 'dm_business',
# MAGIC   tempDir 'wasbs://sqldw@abfsowshq.blob.core.windows.net/tempDirs1'
# MAGIC )
# MAGIC AS SELECT * FROM dim_business;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE TABLE dm_date_1
# MAGIC USING com.databricks.spark.sqldw
# MAGIC OPTIONS (
# MAGIC   url 'jdbc:sqlserver://owshqsqldw.database.windows.net:1433;database=OwsHQDw;user=luanmoreno@owshqsqldw;password={qq11ww22!!@@};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;',
# MAGIC   forwardSparkAzureStorageCredentials 'true',
# MAGIC   dbTable 'dm_date_1',
# MAGIC   tempDir 'wasbs://sqldw@abfsowshq.blob.core.windows.net/tempDirs'
# MAGIC )
# MAGIC AS SELECT * FROM dim_date;

# COMMAND ----------

