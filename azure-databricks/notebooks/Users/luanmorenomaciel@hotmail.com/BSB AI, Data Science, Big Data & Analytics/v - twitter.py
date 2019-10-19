# Databricks notebook source
# MAGIC %scala
# MAGIC import java.util._
# MAGIC     import scala.collection.JavaConverters._
# MAGIC     import com.microsoft.azure.eventhubs._
# MAGIC     import java.util.concurrent._
# MAGIC 
# MAGIC     val namespaceName = "demoowshq"
# MAGIC     val eventHubName = "demoowshq"
# MAGIC     val sasKeyName = "demopolicy"
# MAGIC     val sasKey = "KqvY/m4EWsASP4Utl6Eo114JQOs/C5+85WyuGlvReY0="
# MAGIC     val connStr = new ConnectionStringBuilder()
# MAGIC                 .setNamespaceName(namespaceName)
# MAGIC                 .setEventHubName(eventHubName)
# MAGIC                 .setSasKeyName(sasKeyName)
# MAGIC                 .setSasKey(sasKey)
# MAGIC 
# MAGIC     val pool = Executors.newFixedThreadPool(1)
# MAGIC     val eventHubClient = EventHubClient.create(connStr.toString(), pool)
# MAGIC 
# MAGIC     def sendEvent(message: String) = {
# MAGIC       val messageData = EventData.create(message.getBytes("UTF-8"))
# MAGIC       eventHubClient.get().send(messageData)
# MAGIC       System.out.println("Sent event: " + message + "\n")
# MAGIC     }
# MAGIC 
# MAGIC     import twitter4j._
# MAGIC     import twitter4j.TwitterFactory
# MAGIC     import twitter4j.Twitter
# MAGIC     import twitter4j.conf.ConfigurationBuilder
# MAGIC 
# MAGIC     // Twitter configuration!
# MAGIC     // Replace values below with yours
# MAGIC 
# MAGIC     val twitterConsumerKey = "xlPOZAuQoeVDsXzoBck3b0tzp"
# MAGIC     val twitterConsumerSecret = "cDGnkfJRsZgq60Emp3OYEcle6LldwRqHtXzmj7sZqeZQovvKZi"
# MAGIC     val twitterOauthAccessToken = "1045030220330397696-EkwpeLpmvtVaC4iCs4r2PUs5tPqPuH"
# MAGIC     val twitterOauthTokenSecret = "v0ScjA2RfGZnupEBwdhXYWqkIBu8lLB6Y60LnJj5gElpL"
# MAGIC 
# MAGIC     val cb = new ConfigurationBuilder()
# MAGIC       cb.setDebugEnabled(true)
# MAGIC       .setOAuthConsumerKey(twitterConsumerKey)
# MAGIC       .setOAuthConsumerSecret(twitterConsumerSecret)
# MAGIC       .setOAuthAccessToken(twitterOauthAccessToken)
# MAGIC       .setOAuthAccessTokenSecret(twitterOauthTokenSecret)
# MAGIC 
# MAGIC     val twitterFactory = new TwitterFactory(cb.build())
# MAGIC     val twitter = twitterFactory.getInstance()
# MAGIC 
# MAGIC     // Getting tweets with keyword "Azure" and sending them to the Event Hub in realtime!
# MAGIC 
# MAGIC     val query = new Query("#Yelp")
# MAGIC     query.setCount(100)
# MAGIC     query.lang("en")
# MAGIC     var finished = false
# MAGIC     while (!finished) {
# MAGIC       val result = twitter.search(query)
# MAGIC       val statuses = result.getTweets()
# MAGIC       var lowestStatusId = Long.MaxValue
# MAGIC       for (status <- statuses.asScala) {
# MAGIC         if(!status.isRetweet()){
# MAGIC           sendEvent(status.getText())
# MAGIC         }
# MAGIC         lowestStatusId = Math.min(status.getId(), lowestStatusId)
# MAGIC         Thread.sleep(2000)
# MAGIC       }
# MAGIC       query.setMaxId(lowestStatusId - 1)
# MAGIC     }
# MAGIC 
# MAGIC     // Closing connection to the Event Hub
# MAGIC     eventHubClient.get().close()

# COMMAND ----------

