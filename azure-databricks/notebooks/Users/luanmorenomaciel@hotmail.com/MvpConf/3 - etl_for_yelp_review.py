# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC **reviews** data is keep coming from data lake to apache spark (databricks), using streaming capabilities on spark to load and work with databricks delta storage

# COMMAND ----------

# DBTITLE 1,Listing New Files - Arrival Date - Reviews from API
# MAGIC %fs ls "dbfs:/mnt/brzluanmoreno/yelp_reviews"

# COMMAND ----------

# DBTITLE 1,Structured Streams - Schema Definition for Streaming Processing
from pyspark.sql.types import *

inputPath = "dbfs:/mnt/brzluanmoreno/yelp_reviews"

jsonSchema = StructType(
[
 StructField('business_id', StringType(), True), 
 StructField('cool', LongType(), True), 
 StructField('date', StringType(), True), 
 StructField('funny', LongType(), True), 
 StructField('review_id', StringType(), True), 
 StructField('stars', LongType(), True), 
 StructField('text', StringType(), True), 
 StructField('useful', LongType(), True), 
 StructField('user_id', StringType(), True)
]
)

StreamingDfReviews = (
     spark
    .readStream
    .schema(jsonSchema)
    .option("maxFilesPerTrigger", 1)
    .json(inputPath)
)

StreamingDfReviews.isStreaming

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC As you can see, **StreamingDfReviews** is a streaming Dataframe (StreamingDfReviews.isStreaming was true). You can start streaming computation, by defining the **sink** and starting it.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC As a **sink**
# MAGIC You can also write data into a **Databricks Delta** table using Structured Streaming. The transaction log enables Databricks Delta to guarantee exactly-once processing, even when there are other streams or batch queries running concurrently against the table.

# COMMAND ----------

# DBTITLE 1,Configuring & Ingesting Streaming Processing into Databricks Delta Table
spark.conf.set("spark.sql.shuffle.partitions", "2")  

query = (
  StreamingDfReviews
    .writeStream
    .format("delta")  
    .outputMode("append")
    .option("checkpointLocation", "/delta/reviews/_checkpoints/etl-from-json-mvpconf2019")
    .start("/delta/reviews")
)

# COMMAND ----------

from time import sleep
sleep(10)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC WITH data_summary_reviews AS 
# MAGIC (
# MAGIC SELECT date,
# MAGIC        SUM(stars) AS stars,
# MAGIC        COUNT(*) AS reviews
# MAGIC FROM delta.`/delta/ss_reviews`
# MAGIC WHERE date BETWEEN '2018-01-01' AND '2018-02-01'
# MAGIC GROUP BY date
# MAGIC )
# MAGIC SELECT date,
# MAGIC        reviews
# MAGIC FROM data_summary_reviews
# MAGIC ORDER BY date ASC

# COMMAND ----------

from time import sleep 
sleep(30)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC WITH data_summary_reviews AS 
# MAGIC (
# MAGIC SELECT date,
# MAGIC        SUM(stars) AS stars,
# MAGIC        COUNT(*) AS reviews
# MAGIC FROM delta.`/delta/ss_reviews`
# MAGIC WHERE date BETWEEN '2018-01-01' AND '2018-02-01'
# MAGIC GROUP BY date
# MAGIC )
# MAGIC SELECT date,
# MAGIC        reviews
# MAGIC FROM data_summary_reviews
# MAGIC ORDER BY date ASC

# COMMAND ----------

# DBTITLE 1,Reading using Python API
df_delta_reviews = spark.read.format("delta").load("/delta/ss_reviews")
display(df_delta_reviews)

# COMMAND ----------

