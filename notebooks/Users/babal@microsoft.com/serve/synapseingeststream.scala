// Databricks notebook source
Class.forName("com.databricks.spark.sqldw.DefaultSource")

// COMMAND ----------

import scala.collection.JavaConverters._
import com.microsoft.azure.eventhubs._
import java.util.concurrent._
import scala.collection.immutable._
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

import org.apache.spark.sql.functions._

// COMMAND ----------

print getArgument("tableid", "2")

// COMMAND ----------

spark.conf.set(   "fs.azure.account.key.waginput.blob.core.windows.net", "TBwLoPOLim87APX5grtZzWy8Td9h69F/BJgDxuiQyEP480Cs5zyOa2bUeVVRfUnCALOug3aA2Wb4cj8aIiqGEw==")

// COMMAND ----------

val checkpointLocation = "wasbs://deltaidi@waginput.blob.core.windows.net/deltaidi/delta_custdata2/_checkpoints/etl-from-json"
val deltapath = "wasbs://deltaidi@waginput.blob.core.windows.net/deltaidi/delta_custdata2"
val checkpointLocationforsqldw = "wasbs://deltaidi@waginput.blob.core.windows.net/deltaidi/delta_custdata2/_checkpoints/etl-from-json_sqldw1"

// COMMAND ----------

import org.apache.spark.sql.SaveMode

// COMMAND ----------

val jdbcconn = "jdbc:sqlserver://idicdmsvr.database.windows.net:1433;database=idicdm;user=sqladmin@idicdmsvr;password=Azure!2345678;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"

// COMMAND ----------

dbutils.fs.rm("wasbs://deltaidi@waginput.blob.core.windows.net/deltaidi/delta_custdata2/_checkpoints/etl-from-json_sqldw1", true)

// COMMAND ----------

val messages = spark.readStream.format("delta").option("ignoreDeletes", "true").option("ignoreChanges", "true").option("path", deltapath).table("delta_custdata").select("eventdatetime","customername","address","city","state","zip")

// COMMAND ----------

//messages.writeStream.select("eventdatetime","customername","address","city","state","zip")
messages.writeStream
  .format("com.databricks.spark.sqldw")
  .option("url", jdbcconn)
  .option("tempDir", "wasbs://iditemp@waginput.blob.core.windows.net/stage")
  .option("forwardSparkAzureStorageCredentials", "true")
  .option("dbTable", "custdata2")
  .option("checkpointLocation", checkpointLocationforsqldw)
  .start()