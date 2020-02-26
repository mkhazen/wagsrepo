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

spark.conf.set(   "fs.azure.account.key.waginput.blob.core.windows.net", "TBwLoPOLim87APX5grtZzWy8Td9h69F/BJgDxuiQyEP480Cs5zyOa2bUeVVRfUnCALOug3aA2Wb4cj8aIiqGEw==")

// COMMAND ----------

val checkpointLocation = "wasbs://deltaidi@waginput.blob.core.windows.net/deltaidi/delta_custdata/_checkpoints/etl-from-json"
val deltapath = "wasbs://deltaidi@waginput.blob.core.windows.net/deltaidi/delta_custdata"
val checkpointLocationforsqldw = "wasbs://deltaidi@waginput.blob.core.windows.net/deltaidi/delta_custdata/_checkpoints/etl-from-json_sqldw"

// COMMAND ----------

dbutils.fs.rm("wasbs://deltaidi@waginput.blob.core.windows.net/deltaidi/delta_custdata/_checkpoints/etl-from-json_sqldw", true)

// COMMAND ----------

val messages = spark.readStream.format("delta").option("ignoreDeletes", "true").option("ignoreChanges", "true").option("path", deltapath).table("delta_custdata")

// COMMAND ----------

val jdbcUsername = dbutils.secrets.get(scope = "jdbc", key = "username")
val jdbcPassword = dbutils.secrets.get(scope = "jdbc", key = "password")

// COMMAND ----------

val jdbcHostname = "idicdmsvr.database.windows.net"
val jdbcPort = 1433
val jdbcDatabase = "idicdm"

// Create the JDBC URL without passing in the user and password parameters.
val jdbcUrl = s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}"

// Create a Properties() object to hold the parameters.
import java.util.Properties
val connectionProperties = new Properties()

connectionProperties.put("user", s"sqladmin")
connectionProperties.put("password", s"Azure!2345678")

// COMMAND ----------

val driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
connectionProperties.setProperty("Driver", driverClass)

val jdbcconn = "jdbc:sqlserver://idicdmsvr.database.windows.net:1433;database=idicdm;user=sqladmin@idicdmsvr;password=Azure!2345678;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"

// COMMAND ----------

messages.writeStream..select("eventdatetime","customername","address","city","state","zip")
  .format("com.databricks.spark.sqldw")
  .option("url", jdbcconn)
  .option("tempDir", "wasbs://iditemp@waginput.blob.core.windows.net/stage")
  .option("forwardSparkAzureStorageCredentials", "true")
  .option("dbTable", "custdata1")
  .option("checkpointLocation", checkpointLocationforsqldw)
  .start()