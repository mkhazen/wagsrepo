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
import org.apache.spark.sql.SaveMode

// COMMAND ----------

dbutils.widgets.text("tableid", "20")
val tableid = dbutils.widgets.get("tableid")

// COMMAND ----------

print(tableid)

// COMMAND ----------

val blobname = dbutils.secrets.get(scope = "wagsdata", key = "blobname")
val blobkey = dbutils.secrets.get(scope = "wagsdata", key = "blobkey")
val scmcosmoscontainer = dbutils.secrets.get(scope = "wagsdata", key = "scmcosmoscontainer")
val blobcontainer = dbutils.secrets.get(scope = "wagsdata", key = "blobcontainer")

val sqldwname1 = dbutils.secrets.get(scope = "wagsdata", key = "sqldwname1")
val sqldwuser1 = dbutils.secrets.get(scope = "wagsdata", key = "sqldwuser1")
val sqldwpwd1 = dbutils.secrets.get(scope = "wagsdata", key = "sqldwpwd1")
val sqldwsvrname1 = dbutils.secrets.get(scope = "wagsdata", key = "sqldwsvrname1")
val sqldwdatabase1 = dbutils.secrets.get(scope = "wagsdata", key = "sqldwdatabase1")


// COMMAND ----------

spark.conf.set(   "fs.azure.account.key."+ blobname +".blob.core.windows.net", blobkey)

// COMMAND ----------

val bloburl = "wasbs://"+blobcontainer+"@"+blobname+".blob.core.windows.net"

// COMMAND ----------

val checkpointLocation = bloburl + "/deltaidi/delta_custdata" + tableid + "/_checkpoints/etl-from-json"
//val deltapath = "wasbs://deltaidi@waginput.blob.core.windows.net/deltaidi/delta_custdata" + tableid
val deltapath = bloburl + "/deltaidi/delta_custdata"
val checkpointLocationforsqldw = bloburl + "/deltaidi/delta_custdata" + tableid + "/_checkpoints/etl-from-json_sqldw1"

// COMMAND ----------

val jdbcconn = "jdbc:sqlserver://" + sqldwname1 + ":1433;database=" + sqldwdatabase1 + ";user=" + sqldwuser1 + "@" + sqldwsvrname1 + ";password=" + sqldwpwd1 + ";encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"

// COMMAND ----------

dbutils.fs.rm(bloburl + "/deltaidi/delta_custdata"+ tableid +"/_checkpoints/etl-from-json_sqldw1", true)

// COMMAND ----------

val messages = spark.readStream.format("delta").option("ignoreDeletes", "true").option("ignoreChanges", "true").option("path", deltapath).table("delta_custdata").select("eventdatetime","customername","address","city","state","zip")

// COMMAND ----------

//messages.writeStream.select("eventdatetime","customername","address","city","state","zip")
val tablename = "custdata" + tableid
messages.writeStream
  .format("com.databricks.spark.sqldw")
  .option("url", jdbcconn)
  .option("tempDir", "wasbs://iditemp@waginput.blob.core.windows.net/stage")
  .option("forwardSparkAzureStorageCredentials", "true")
  .option("dbTable", tablename)
  .option("checkpointLocation", checkpointLocationforsqldw)
  .start()