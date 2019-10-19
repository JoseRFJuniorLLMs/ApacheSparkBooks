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
# MAGIC 1. ML

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC ### Ingest DataSet
# MAGIC 
# MAGIC Explore File System

# COMMAND ----------

# MAGIC %fs ls "dbfs:/mnt/prod-files/"

# COMMAND ----------

# MAGIC %md
# MAGIC Read Parquet Files

# COMMAND ----------

ds_review = sqlContext.read.parquet("dbfs:/mnt/prod-files/yelp_review.parquet/")
display(ds_review)

# COMMAND ----------

ds_review = ds_review.select(['business_id', 'user_id', 'text', 'stars', 'date'])
display(ds_review)

# COMMAND ----------

ds_user = sqlContext.read.parquet("dbfs:/mnt/prod-files/yelp_user.parquet/")
display(ds_user)

# COMMAND ----------

ds_user = ds_user.select(['user_id', 'name']).withColumnRenamed("name", "user")
display(ds_user)

# COMMAND ----------

ds_business = sqlContext.read.parquet("dbfs:/mnt/prod-files/yelp_business.parquet/")
display(ds_business)

# COMMAND ----------

ds_business = ds_business.select(['business_id', 'name']).withColumnRenamed("name", "business")
display(ds_business)

# COMMAND ----------

ds = ds_review.join(ds_business,ds_business.business_id == ds_review.business_id, how='left')\
              .join(ds_user,ds_user.user_id == ds_review.user_id, how='left')
display(ds)

# COMMAND ----------

ds = ds.drop('business_id').drop('user_id')
display(ds)

# COMMAND ----------

ds = ds.select(['user', 'business', 'stars', 'date', 'text'])
display(ds)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Explore DataSet

# COMMAND ----------

display(ds.describe('stars', 'date'))

# COMMAND ----------

ds.dataFrame.createOrReplaceTempView("dataset")

# COMMAND ----------

# MAGIC %r
# MAGIC library(SparkR)
# MAGIC devtools::install_github("SKKU-SKT/ggplot2.SparkR")
# MAGIC #library(ggplot2.SparkR)

# COMMAND ----------



# COMMAND ----------

fig, ax = plt.subplots()
ax.plot(stars)
display(fig)