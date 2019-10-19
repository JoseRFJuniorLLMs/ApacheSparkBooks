// Databricks notebook source
// DBTITLE 1,Checking JDBC Driver Availability
Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver")

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ##### Secrets = https://docs.azuredatabricks.net/user-guide/secrets/index.html

// COMMAND ----------

// DBTITLE 1,Creating JDBC URL Connection
val jdbcHostname = "azowshq.database.windows.net"
val jdbcPort = 1433
val jdbcDatabase = "owshqbigdata"
val jdbcUsername = "luanmoreno"
val jdbcPassword = "qq11ww22!!@@"

// create the JDBC URL without passing in the user and password parameters.
val jdbcUrl = s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}"

// create a Properties() object to hold the parameters.
import java.util.Properties
val connectionProperties = new Properties()

connectionProperties.put("user", s"${jdbcUsername}")
connectionProperties.put("password", s"${jdbcPassword}")

// COMMAND ----------

// DBTITLE 1,Check Connectivity Azure SQL Server DB
val driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
connectionProperties.setProperty("Driver", driverClass)

// COMMAND ----------

// DBTITLE 1,Read Table JDBC
val product_table = spark.read.jdbc(jdbcUrl, "SalesLT.product", connectionProperties)

// COMMAND ----------

product_table.createOrReplaceTempView("db_product")

// COMMAND ----------

display(product_table)

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC SELECT *
// MAGIC FROM db_product

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Reading, Writing and Working with JDBC & Azure SQL DB = https://docs.azuredatabricks.net/spark/latest/data-sources/sql-databases.html

// COMMAND ----------

