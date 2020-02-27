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

import org.joda.time._
import org.joda.time.format._
import com.microsoft.azure.cosmosdb.spark.schema._
import com.microsoft.azure.cosmosdb.spark._
import com.microsoft.azure.cosmosdb.spark.config.Config
import org.codehaus.jackson.map.ObjectMapper
import com.microsoft.azure.cosmosdb.spark.streaming._

// COMMAND ----------

spark.conf.set(   "fs.azure.account.key.waginput.blob.core.windows.net", "TBwLoPOLim87APX5grtZzWy8Td9h69F/BJgDxuiQyEP480Cs5zyOa2bUeVVRfUnCALOug3aA2Wb4cj8aIiqGEw==")

// COMMAND ----------

val deltapathproduct = "wasbs://deltaidi@waginput.blob.core.windows.net/deltaidi/delta_productdim"

val deltapathsupplier = "wasbs://deltaidi@waginput.blob.core.windows.net/deltaidi/delta_supplierdim"

val deltapathlocation = "wasbs://deltaidi@waginput.blob.core.windows.net/deltaidi/delta_locationdim"

val deltapathteam = "wasbs://deltaidi@waginput.blob.core.windows.net/deltaidi/delta_teamdim"

// COMMAND ----------

//load dimensions now
val dfproduct = spark.read.format("delta").load(deltapathproduct).select("customername","productname")

// COMMAND ----------

dfproduct.count

// COMMAND ----------

//load dimensions now
val dfsupplier = spark.read.format("delta").load(deltapathsupplier).select("customername","suppliername")
val dflocation = spark.read.format("delta").load(deltapathlocation).select("customername","locationname")
val dfteam = spark.read.format("delta").load(deltapathteam).select("customername","teamname")

// COMMAND ----------

dfsupplier.count
dflocation.count
dfteam.count

// COMMAND ----------

val checkpointLocation = "wasbs://deltaidi@waginput.blob.core.windows.net/deltaidi/delta_custdata/_checkpoints/etl-from-json"
val deltapath = "wasbs://deltaidi@waginput.blob.core.windows.net/deltaidi/delta_custdata"
val checkpointLocationforcosmo1 = "wasbs://deltaidi@waginput.blob.core.windows.net/deltaidi/delta_custdata/_checkpoints/etl-from-json_cosmos1"

// COMMAND ----------

dbutils.fs.rm("wasbs://deltaidi@waginput.blob.core.windows.net/deltaidi/delta_custdata/_checkpoints/etl-from-json_cosmos1", true)

// COMMAND ----------

val messages = spark.readStream.format("delta").option("ignoreDeletes", "true").option("ignoreChanges", "true").option("path", deltapath).table("delta_custdata")

// COMMAND ----------

val ConfigMap = Map(
"Endpoint" -> "https://idicosmos.documents.azure.com:443/",
"Masterkey" -> "zoyDIf201Wpu6kCY0Njrh3R0v9SgUdYKmm3nvilyjYEXkhKv95aTgOBxN7tbSA4nU6iXPfcZh36v9rLL3EaEeg==",
"Database" -> "idiscm",
"Collection" -> "custdata2",
"Upsert" -> "true"
)
//messages.select("eventdatetime","customername","address","city","zip").withColumn("Date", (col("eventdatetime").cast("date"))) 
messages
  .join(dfproduct, "customername")
  .join(dfsupplier, "customername")
  .join(dflocation, "customername")
  .join(dfteam, "customername")
  .select("eventdatetime","customername","address","city","state","zip","productname","suppliername","locationname","teamname")
  .writeStream
  .format(classOf[CosmosDBSinkProvider].getName)
  .outputMode("update")
  .options(ConfigMap)
  .option("checkpointLocation", checkpointLocationforcosmo1)
  .start()

// COMMAND ----------

