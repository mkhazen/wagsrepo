// Databricks notebook source
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

// COMMAND ----------

val custdatadf = spark.table("delta_custdata")

// COMMAND ----------

display(custdatadf)

// COMMAND ----------

custdatadf.count

// COMMAND ----------

// Import Necessary Libraries
import com.microsoft.azure.cosmosdb.spark.schema._
import com.microsoft.azure.cosmosdb.spark._
import com.microsoft.azure.cosmosdb.spark.config.Config

// COMMAND ----------

// Read Configuration
val readConfig = Config(Map(
  "Endpoint" -> "https://idicosmos.documents.azure.com:443/",
  "Masterkey" -> "zoyDIf201Wpu6kCY0Njrh3R0v9SgUdYKmm3nvilyjYEXkhKv95aTgOBxN7tbSA4nU6iXPfcZh36v9rLL3EaEeg==",
  "Database" -> "idiscm",
  "Collection" -> "custdata",
  "query_custom" -> "SELECT * from c" // Optional
))

// Connect via azure-cosmosdb-spark to create Spark DataFrame
val itemdf = spark.read.cosmosDB(readConfig)
itemdf.count()

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
custdatadf.write.mode(SaveMode.Overwrite).cosmosDB(writeConfig)