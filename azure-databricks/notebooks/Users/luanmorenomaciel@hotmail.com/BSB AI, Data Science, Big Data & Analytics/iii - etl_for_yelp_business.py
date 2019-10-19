# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Luan Moreno M. Maciel
# MAGIC ### CDO - Chief Data Officer at One Way Solution
# MAGIC https://www.yelp.com/dataset/challenge
# MAGIC 
# MAGIC 
# MAGIC 1 - Ingest DataSet
# MAGIC <br>
# MAGIC 2 - Explore DataSet
# MAGIC <br>
# MAGIC 3 - Transform DataSet

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 1. Ingest DataSet
# MAGIC 
# MAGIC dataset selected for analysis
# MAGIC <br>
# MAGIC <br>
# MAGIC 
# MAGIC - yelp_business

# COMMAND ----------

# MAGIC %fs ls "dbfs:/mnt/prod-files/"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC sqlContext = The entry point into all relational functionality in Spark is the SQLContext class, or one of its decedents. To create a basic SQLContext, all you need is a SparkContext.
# MAGIC <br>
# MAGIC https://spark.apache.org/docs/1.6.1/sql-programming-guide.html

# COMMAND ----------

ds_business = sqlContext.read.parquet("dbfs:/mnt/prod-files/yelp_business.parquet/")
display(ds_business)

# COMMAND ----------

ds_business.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC importing classes of spark-sql and dataframes
# MAGIC <br>
# MAGIC https://spark.apache.org/docs/1.6.2/api/python/pyspark.sql.html
# MAGIC 
# MAGIC 
# MAGIC registering a dataframe as table for spark-sql use

# COMMAND ----------

sqlContext.registerDataFrameAsTable(ds_business, "tmp_business")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM tmp_business
# MAGIC LIMIT 100;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE TABLE tmp_business

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 2. Explore DataSet
# MAGIC 
# MAGIC dataset selected for analysis
# MAGIC <br>
# MAGIC <br>
# MAGIC 
# MAGIC - yelp_business

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC performing a set of **data exploration** on the data to understand about it, in this case we're going to showcase a study about the user inside of the **yelp** platform, as you can see the user (json) file has interesting values about the user against the reviews, useful, status and how many fans they have.

# COMMAND ----------

ft_business = ds_business.select(['business_id','name','categories','city','state','address','latitude','longitude','review_count','stars'])
display(ft_business)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC performing a series of data exploration using spark-sql

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### cities that most use yelp based on reviews

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT COUNT(*)
# MAGIC FROM tmp_business

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT city, SUM(review_count)
# MAGIC FROM tmp_business
# MAGIC GROUP BY city
# MAGIC ORDER BY SUM(review_count) DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md 
# MAGIC #### verifying that the most reviewed cities are not necessarily the best ones

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC WITH reviews AS 
# MAGIC (
# MAGIC SELECT city, SUM(review_count) AS review_count
# MAGIC FROM tmp_business
# MAGIC GROUP BY city
# MAGIC HAVING SUM(review_count) > 1000
# MAGIC )
# MAGIC SELECT rv.city, AVG(b.stars) AS stars
# MAGIC FROM reviews AS rv
# MAGIC INNER JOIN tmp_business AS b
# MAGIC ON rv.city = b.city
# MAGIC GROUP BY rv.city
# MAGIC ORDER BY stars DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md 
# MAGIC #### map plot 

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT state,
# MAGIC        AVG(stars)
# MAGIC FROM tmp_business
# MAGIC WHERE state NOT IN ('11', 'WAR', '01', 'NYK', 'NW', 'HH', 'QC', 'B', 'BC', 'M', 'V', 'BY', '6', 
# MAGIC 'SP', 'O', 'PO', 'XMS', 'C', 'XGM', 'CC', 'VS', 'RP', 'AG', 'SG', 'TAM', 'ON', 'AB', 'G', 'CS', 'RCC', 'HU', '10', 
# MAGIC '4', 'NI', 'NLK', 'HE', 'CMA', 'LU', 'WHT', '45', 'ST', 'CRF')
# MAGIC   AND review_count >= "$reviews"
# MAGIC GROUP BY state

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 3. Transform DataSet
# MAGIC 
# MAGIC dataset selected for analysis
# MAGIC <br>
# MAGIC <br>
# MAGIC 
# MAGIC - yelp_business

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM tmp_business
# MAGIC LIMIT 100

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE stg_business
# MAGIC AS
# MAGIC SELECT business_id,
# MAGIC        name,
# MAGIC        city, 
# MAGIC        state,
# MAGIC        regexp_extract(categories,"^(.+?),") AS category,
# MAGIC        categories AS subcategories,
# MAGIC        review_count,
# MAGIC        stars
# MAGIC FROM tmp_business

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * 
# MAGIC FROM stg_business
# MAGIC LIMIT 100;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC actions performed in this notebook
# MAGIC <br>
# MAGIC <br>
# MAGIC 
# MAGIC - loaded data using parquet format
# MAGIC - exploration of the dataset 
# MAGIC - performing transformations in the dataset, adding new column and data validation
# MAGIC 
# MAGIC stage table = stg_business 
# MAGIC 
# MAGIC **ready for deployment**

# COMMAND ----------

