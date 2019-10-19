# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Data Engineering Deliverables  
# MAGIC 
# MAGIC 
# MAGIC 1 - Ingest DataSet
# MAGIC <br>
# MAGIC 2 - Explore DataSet
# MAGIC <br>
# MAGIC 3 - Transform DataSet
# MAGIC <br>
# MAGIC 4 - Consolidation

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM yelp_users_delta
# MAGIC LIMIT 100

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM delta_business
# MAGIC LIMIT 100

# COMMAND ----------

df_delta_reviews = spark.read.format("delta").load("/delta/ss_reviews")
display(df_delta_reviews)


# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE TABLE reviews 
# MAGIC (
# MAGIC business_id STRING,
# MAGIC cool BIGINT,
# MAGIC date STRING,
# MAGIC funny BIGINT,
# MAGIC review_id STRING,
# MAGIC stars BIGINT,
# MAGIC text STRING,
# MAGIC useful BIGINT,
# MAGIC user_id STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION '/delta/reviews'

# COMMAND ----------

# MAGIC %sql
# MAGIC --107.945.928
# MAGIC 
# MAGIC SELECT COUNT(*)
# MAGIC FROM reviews

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM reviews AS r
# MAGIC INNER JOIN delta_business AS b
# MAGIC ON r.business_id = b.business_id
# MAGIC INNER JOIN yelp_users_delta AS u
# MAGIC ON r.user_id = u.user_id
# MAGIC LIMIT 100

# COMMAND ----------

