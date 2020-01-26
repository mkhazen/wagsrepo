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

val config = Config(Map(
  "url"            -> "idisvr.database.windows.net",
  "databaseName"   -> "idi",
  "dbTable"        -> "dbo.YellowCab_Rawdata",
  "user"           -> "sqladmin",
  "password"       -> "Azure!2345678",
  "connectTimeout" -> "5", //seconds
  "queryTimeout"   -> "5"  //seconds
))

val collection = sqlContext.read.sqlDB(config)
collection.show()

// COMMAND ----------

import org.apache.spark.sql.SaveMode
df.write.mode(SaveMode.Append).sqlDB(config)