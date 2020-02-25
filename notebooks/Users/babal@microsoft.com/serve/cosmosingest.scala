// Databricks notebook source
import scala.collection.JavaConverters._
import com.microsoft.azure.eventhubs._
import java.util.concurrent._
import scala.collection.immutable._
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

import org.apache.spark.sql.functions._

// Import Necessary Libraries
import com.microsoft.azure.cosmosdb.spark.schema._
import com.microsoft.azure.cosmosdb.spark._
import com.microsoft.azure.cosmosdb.spark.config.Config


// COMMAND ----------

val messages = spark.readStream.format("delta").table("delta_custdata")

// COMMAND ----------

// Write configuration
val writeConfig = Config(Map(
  "Endpoint" -> "https://idicosmos.documents.azure.com:443/",
  "Masterkey" -> "zoyDIf201Wpu6kCY0Njrh3R0v9SgUdYKmm3nvilyjYEXkhKv95aTgOBxN7tbSA4nU6iXPfcZh36v9rLL3EaEeg==",
  "Database" -> "idiscm",
  "Collection" -> "custdata",
  "WritingBatchSize" -> "5"
))

// Write to Cosmos DB from the flights DataFrame
import org.apache.spark.sql.SaveMode
messages.repartition(1).write.mode(SaveMode.Overwrite).cosmosDB(writeConfig)