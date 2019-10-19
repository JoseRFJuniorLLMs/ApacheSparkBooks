# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Matheus Willian
# MAGIC ## Data Scientist at One Way Solution
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC ## Yelp Dataset = [Reviews]
# MAGIC https://www.yelp.com/dataset/challenge
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC ## Plan
# MAGIC 
# MAGIC <br>
# MAGIC 
# MAGIC 1. Ingest DataSet
# MAGIC 1. Explore DataSet
# MAGIC 1. Transform DataSet

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC 
# MAGIC ## Ingest DataSet

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC exploring dataset

# COMMAND ----------

# MAGIC %fs ls "dbfs:/mnt/prod-files/"

# COMMAND ----------

# MAGIC %md
# MAGIC read parquet file

# COMMAND ----------

ds_review = sqlContext.read.parquet("dbfs:/mnt/prod-files/yelp_review.parquet/")
display(ds_review)

# COMMAND ----------

ds_review = ds_review.drop('text')
display(ds_review)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Explore DataSet 

# COMMAND ----------

import ggplot


# COMMAND ----------

# MAGIC %md
# MAGIC ### Transform DataSet

# COMMAND ----------

# MAGIC %md
# MAGIC date dimension

# COMMAND ----------

max_date = ds_review.select('date').rdd.max()[0].encode('ascii')
min_date = ds_review.select('date').rdd.min()[0].encode('ascii')
print(min_date, max_date)

# COMMAND ----------

import pandas as pd

def create_dim_date(start, end):
  df = pd.DataFrame({"date": pd.date_range(start, end)})
  df["week_day"] = df.date.dt.weekday_name
  df["day"]      = df.date.dt.day
  df["month"]    = df.date.dt.month
  df["year"]     = df.date.dt.year
  return df

# COMMAND ----------

dim_date = create_dim_date(min_date, max_date)
dim_date.head()

# COMMAND ----------

dim_date.date.describe()

# COMMAND ----------

dim_date.date = dim_date.date.dt.date.astype(str)
dim_date.date.describe()

# COMMAND ----------

dim_date = spark.createDataFrame(dim_date)
display(dim_date)

# COMMAND ----------

dim_date.createOrReplaceTempView("stg_date")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM stg_date

# COMMAND ----------

# MAGIC %scala
# MAGIC table("stg_date").write.mode(SaveMode.Overwrite).saveAsTable("stg_date")

# COMMAND ----------

# MAGIC %md
# MAGIC fact review

# COMMAND ----------

ds_review.createOrReplaceTempView("ft_review")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM ft_review

# COMMAND ----------

# MAGIC %scala
# MAGIC table("ft_review").write.mode(SaveMode.Overwrite).saveAsTable("ft_review")