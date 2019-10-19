# Databricks notebook source
# MAGIC %scala
# MAGIC import org.apache.spark.eventhubs._
# MAGIC 
# MAGIC     // Build connection string with the above information
# MAGIC     val connectionString = ConnectionStringBuilder("Endpoint=sb://demoowshq.servicebus.windows.net/;SharedAccessKeyName=demopolicy;SharedAccessKey=KqvY/m4EWsASP4Utl6Eo114JQOs/C5+85WyuGlvReY0=;EntityPath=demoowshq")
# MAGIC       .setEventHubName("demoowshq")
# MAGIC       .build
# MAGIC 
# MAGIC     val customEventhubParameters =
# MAGIC       EventHubsConf(connectionString)
# MAGIC       .setMaxEventsPerTrigger(5)
# MAGIC 
# MAGIC     val incomingStream = spark.readStream.format("eventhubs").options(customEventhubParameters.toMap).load()
# MAGIC 
# MAGIC     incomingStream.printSchema
# MAGIC 
# MAGIC     // Sending the incoming stream into the console.
# MAGIC     // Data comes in batches!
# MAGIC     incomingStream.writeStream.outputMode("append").format("console").option("truncate", false).start().awaitTermination()

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.DataFrame
# MAGIC import org.apache.spark.sql.types._
# MAGIC     import org.apache.spark.sql.functions._
# MAGIC import org.apache.spark.eventhubs._
# MAGIC 
# MAGIC     // Build connection string with the above information
# MAGIC     val connectionString = ConnectionStringBuilder("Endpoint=sb://demoowshq.servicebus.windows.net/;SharedAccessKeyName=demopolicy;SharedAccessKey=KqvY/m4EWsASP4Utl6Eo114JQOs/C5+85WyuGlvReY0=;EntityPath=demoowshq")
# MAGIC       .setEventHubName("demoowshq")
# MAGIC       .build
# MAGIC 
# MAGIC     val customEventhubParameters =
# MAGIC       EventHubsConf(connectionString)
# MAGIC       .setMaxEventsPerTrigger(5)
# MAGIC 
# MAGIC     val incomingStream = spark.readStream.format("eventhubs").options(customEventhubParameters.toMap).load()
# MAGIC 
# MAGIC     // Event Hub message format is JSON and contains "body" field
# MAGIC     // Body is binary, so we cast it to string to see the actual content of the message
# MAGIC     val messages =
# MAGIC       incomingStream
# MAGIC       .withColumn("Offset", $"offset".cast(LongType))
# MAGIC       .withColumn("Time (readable)", $"enqueuedTime".cast(TimestampType))
# MAGIC       .withColumn("Timestamp", $"enqueuedTime".cast(LongType))
# MAGIC       .withColumn("Body", $"body".cast(StringType))
# MAGIC       .select("Offset", "Time (readable)", "Timestamp", "Body")
# MAGIC 
# MAGIC 
# MAGIC val df: DataFrame = messages
# MAGIC // Apply some transformations to the data then use
# MAGIC // Structured Streaming API to continuously write the data to a table in SQL DW.
# MAGIC 
# MAGIC   df.printSchema
# MAGIC   
# MAGIC   df.writeStream.outputMode("append")
# MAGIC   .format("com.databricks.spark.sqldw")
# MAGIC   .option("url", "jdbc:sqlserver://owshqsqldw.database.windows.net:1433;database=OwsHQDw;user=luanmoreno@owshqsqldw;password={qq11ww22!!@@};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;")
# MAGIC   .option("tempDir", "wasbs://sqldw@abfsowshq.blob.core.windows.net/tempDirs")
# MAGIC   .option("forwardSparkAzureStorageCredentials", "true")
# MAGIC   .option("dbTable", "twitter_stream2")
# MAGIC   .option("checkpointLocation", "/tmp_checkpoint_location")
# MAGIC   .start()
# MAGIC 
# MAGIC     messages.printSchema
# MAGIC     messages.writeStream.outputMode("append").format("console").option("truncate", false).start().awaitTermination()

# COMMAND ----------



    