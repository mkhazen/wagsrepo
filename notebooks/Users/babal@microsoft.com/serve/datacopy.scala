// Databricks notebook source
sc.version

// COMMAND ----------

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

// COMMAND ----------

import com.microsoft.azure.sqldb.spark.config.Config
import com.microsoft.azure.sqldb.spark.connect._

// COMMAND ----------

spark.conf.set(
  "fs.azure.account.key.taxidata.blob.core.windows.net",
  "YCATJJhmBVjpJvZrbyiFhN3UH4LxJy6U74o8Gbpwh5WXDE9OBSRFySa45ABa3hLWoBPcQcJtXqGOvHxhJ6RmmA==")

// COMMAND ----------

val df = spark.read.option("header","true").option("inferSchema","true").parquet("wasbs://sampledatasets@taxidata.blob.core.windows.net/nyctlc/yellow/*/*/*")

// COMMAND ----------

display(df)

// COMMAND ----------

df.count

// COMMAND ----------

val dfvendor = df.select($"vendorID").distinct().show(100)

// COMMAND ----------

val df1 = df.withColumn("Date", (col("tpepPickupDateTime").cast("date")))

// COMMAND ----------

val df2 = df1.withColumn("year", year(col("date"))) .withColumn("month", month(col("date"))) .withColumn("day", dayofmonth(col("date"))) .withColumn("hour", hour(col("date")))

// COMMAND ----------

val df3 = df2.groupBy("year","month").agg(sum("fareAmount").alias("Total"),count("vendorID").alias("Count")).sort(asc("year"), asc("month"))

// COMMAND ----------

val config = Config(Map(
  "url"            -> "idisvr.database.windows.net",
  "databaseName"   -> "idi",
  "dbTable"        -> "dbo.aggrdata",
  "user"           -> "sqladmin",
  "password"       -> "Azure!2345678",
  "connectTimeout" -> "5", //seconds
  "queryTimeout"   -> "5"  //seconds
))

val collection = sqlContext.read.sqlDB(config)
collection.show()

// COMMAND ----------

import org.apache.spark.sql.SaveMode
df3.write.mode(SaveMode.Append).sqlDB(config)

// COMMAND ----------

val writeconfig = Config(Map(
  "url"            -> "idisvr.database.windows.net",
  "databaseName"   -> "idi",
  "dbTable"        -> "dbo.aggrdata",
  "user"           -> "sqladmin",
  "password"       -> "Azure!2345678",
  "connectTimeout" -> "10", //seconds
  "queryTimeout"   -> "60"  //seconds
))

import org.apache.spark.sql.SaveMode
//df3.repartition(10).write.mode(SaveMode.Append).sqlDB(writeconfig)

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
  "Collection" -> "item",
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
  "Collection" -> "item"
))

// Write to Cosmos DB from the flights DataFrame
import org.apache.spark.sql.SaveMode
df3.write.mode(SaveMode.Overwrite).cosmosDB(writeConfig)