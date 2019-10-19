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
# MAGIC - yelp_user

# COMMAND ----------

# MAGIC %fs ls "dbfs:/mnt/prod-files/"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC sqlContext = The entry point into all relational functionality in Spark is the SQLContext class, or one of its decedents. To create a basic SQLContext, all you need is a SparkContext.
# MAGIC <br>
# MAGIC https://spark.apache.org/docs/1.6.1/sql-programming-guide.html

# COMMAND ----------

ds_user = sqlContext.read.parquet("dbfs:/mnt/prod-files/yelp_user.parquet/")
display(ds_user)

# COMMAND ----------

ds_user.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC using pure SQL to load **yelp_user** parquet file, spark-sql in action here

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE IF EXISTS yelp_user;
# MAGIC 
# MAGIC CREATE TEMPORARY TABLE yelp_user
# MAGIC (
# MAGIC average_stars double,
# MAGIC compliment_cool long,
# MAGIC compliment_cute long,
# MAGIC compliment_funny long,
# MAGIC compliment_hot long,
# MAGIC compliment_list long,
# MAGIC compliment_more long,
# MAGIC compliment_note long,
# MAGIC compliment_photos long,
# MAGIC compliment_plain long,
# MAGIC compliment_profile long,
# MAGIC compliment_writer long,
# MAGIC cool long,
# MAGIC elite string,
# MAGIC fans long,
# MAGIC friends string,
# MAGIC funny long,
# MAGIC name string,
# MAGIC review_count long,
# MAGIC useful long,
# MAGIC user_id string,
# MAGIC yelping_since string
# MAGIC )
# MAGIC USING parquet 
# MAGIC OPTIONS
# MAGIC (
# MAGIC   path = "dbfs:/mnt/prod-files/yelp_user.parquet/"
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM yelp_user
# MAGIC LIMIT 100;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC **explain**
# MAGIC 
# MAGIC provide detailed plan information about statement without actually running it. by default this only outputs information about the physical plan.
# MAGIC <br>
# MAGIC A logical plan is a tree that represents both schema and data. These trees are manipulated and optimized by catalyst framework.

# COMMAND ----------

sql("SELECT * FROM yelp_user").explain()

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE TABLE yelp_user

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC EXPLAIN EXTENDED SELECT * FROM yelp_user WHERE average_stars > 3 AND compliment_funny <> 0

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 2. Explore DataSet
# MAGIC 
# MAGIC dataset selected for analysis
# MAGIC <br>
# MAGIC <br>
# MAGIC 
# MAGIC - yelp_user

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC performing a set of **data exploration** on the data to understand about it, in this case we're going to showcase a study about the user inside of the **yelp** platform, as you can see the user (json) file has interesting values about the user against the reviews, useful, status and how many fans they have.

# COMMAND ----------

display(ds_user.where("average_stars > 3"))

# COMMAND ----------

display(ds_user.select("elite"))

# COMMAND ----------

display(ds_user.select("useful").describe())

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql import functions as f
display(ds_user.groupBy("elite").agg(f.avg("review_count")).orderBy("elite"))

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT elite, avg(review_count) as review_count
# MAGIC FROM yelp_user
# MAGIC GROUP BY elite
# MAGIC ORDER BY review_count DESC

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC using **ctas** expresion to create our table

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE IF EXISTS ds_yelp;
# MAGIC 
# MAGIC CREATE TABLE ds_yelp
# MAGIC AS
# MAGIC SELECT user_id,
# MAGIC        name,
# MAGIC        average_stars,
# MAGIC        fans,
# MAGIC        review_count,
# MAGIC        useful,
# MAGIC        yelping_since
# MAGIC FROM yelp_user

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM ds_yelp 
# MAGIC LIMIT 1000;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 3. Transform DataSet
# MAGIC 
# MAGIC dataset selected for analysis
# MAGIC <br>
# MAGIC <br>
# MAGIC 
# MAGIC - yelp_user

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC this command bellow **transform** a table that usually was load based in a **csv, tsv, json or even a text file** in a apache **parquet format**, since parquet uses columnar storage, there are a set of benefits by switching the format of the file

# COMMAND ----------

table("yelp_user").write.mode("overwrite").format("parquet").saveAsTable("yelp_user_optimized")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DESC FORMATTED ds_yelp

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC transforming the original dataset and adding new logic based in the **[average_stars,fans,review_count)]**. 
# MAGIC <br>
# MAGIC creating a function that will define the **importance** of a specific user in the platform based in some criterias
# MAGIC <br>
# MAGIC https://docs.databricks.com/spark/latest/spark-sql/udf-in-python.html

# COMMAND ----------

def usr_importance(average_stars,fans,review_count):
  if average_stars >=3 and fans > 50 and review_count >= 15:
    return "rockstar"
  if average_stars <2 and fans < 20 and review_count < 15:
    return "low"
  else:
    return "normal"
spark.udf.register("usr_importance",usr_importance)
  

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT usr_importance(average_stars,fans,review_count), COUNT(*)
# MAGIC FROM ds_yelp
# MAGIC GROUP BY usr_importance(average_stars,fans,review_count);

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC now that the **data munging [data wrangling]** was performed is now time to write this new column in the dataframe
# MAGIC <br>
# MAGIC https://en.wikipedia.org/wiki/Data_wrangling

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC val new_ds_users = table("ds_yelp").selectExpr("usr_importance(average_stars,fans,review_count) importance", "*")
# MAGIC new_ds_users.write.mode(SaveMode.Overwrite).saveAsTable("stg_users")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT importance, COUNT(*)
# MAGIC FROM stg_users
# MAGIC GROUP BY importance

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM stg_users
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
# MAGIC stage table = stg_users 
# MAGIC 
# MAGIC **ready for deployment**

# COMMAND ----------

