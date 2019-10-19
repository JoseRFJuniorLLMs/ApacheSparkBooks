# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC #### JSON to Parquet
# MAGIC https://docs.databricks.com/spark/latest/data-sources/read-parquet.html

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC reading files from **azure blob storage**

# COMMAND ----------

# MAGIC %fs ls "dbfs:/mnt/stg-files/yelp_dataset/"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC reading json files and adding into df = **dataframes**

# COMMAND ----------

df_business = spark.read.json("dbfs:/mnt/stg-files/yelp_dataset/yelp_academic_dataset_business.json")
df_checkin = spark.read.json("dbfs:/mnt/stg-files/yelp_dataset/yelp_academic_dataset_checkin.json")
df_photo = spark.read.json("dbfs:/mnt/stg-files/yelp_dataset/yelp_academic_dataset_photo.json")
df_review = spark.read.json("dbfs:/mnt/stg-files/yelp_dataset/yelp_academic_dataset_review.json")
df_tip = spark.read.json("dbfs:/mnt/stg-files/yelp_dataset/yelp_academic_dataset_tip.json")
df_user = spark.read.json("dbfs:/mnt/stg-files/yelp_dataset/yelp_academic_dataset_user.json")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC converting files to parquet
# MAGIC <br>
# MAGIC http://parquet.apache.org/
# MAGIC 
# MAGIC in a nutshell, **apache parquet** is a columnar storage format available to any project in the hadoop ecosystem, regardless of the choice of data processing framework, data model or programming language.
# MAGIC <br>
# MAGIC here are some of the key advantages to use parquet files
# MAGIC <br>
# MAGIC <br>
# MAGIC - built from the ground up with complex nested data structures in mind
# MAGIC - built to support very efficient compression and encoding schemes
# MAGIC - allows for lower data storage costs and maximized effectiveness of querying data
# MAGIC - less disk IO
# MAGIC - higher scan throughput

# COMMAND ----------

df_business.write.parquet("/mnt/prod-files/yelp_business.parquet")
df_checkin.write.parquet("/mnt/prod-files/yelp_checkin.parquet")
df_photo.write.parquet("/mnt/prod-files/yelp_photo.parquet")
df_review.write.parquet("/mnt/prod-files/yelp_review.parquet")
df_tip.write.parquet("/mnt/prod-files/yelp_tip.parquet")
df_user.write.parquet("/mnt/prod-files/yelp_user.parquet")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC checking files on the new bucket - azure blob storage [DBFS] - databricks file system

# COMMAND ----------

# MAGIC %fs ls "dbfs:/mnt/prod-files/"

# COMMAND ----------

