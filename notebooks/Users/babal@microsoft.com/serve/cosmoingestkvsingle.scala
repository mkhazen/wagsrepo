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

val blobname = dbutils.secrets.get(scope = "wagsdata", key = "blobname")
val blobkey = dbutils.secrets.get(scope = "wagsdata", key = "blobkey")
val scmcosmosuri = dbutils.secrets.get(scope = "wagsdata", key = "scmcosmosuri")
val scmcosmoskey = dbutils.secrets.get(scope = "wagsdata", key = "scmcosmoskey")
val scmcosmosdb = dbutils.secrets.get(scope = "wagsdata", key = "scmcosmosdb")
val scmcosmoscontainer = dbutils.secrets.get(scope = "wagsdata", key = "scmcosmoscontainer")
val blobcontainer = dbutils.secrets.get(scope = "wagsdata", key = "blobcontainer")

// COMMAND ----------

spark.conf.set(   "fs.azure.account.key."+ blobname +".blob.core.windows.net", blobkey)

// COMMAND ----------

val bloburl = "wasbs://"+blobcontainer+"@"+blobname+".blob.core.windows.net"

// COMMAND ----------

val deltapathproduct = bloburl + "/deltaidi/delta_productdim"

val deltapathsupplier = bloburl + "/deltaidi/delta_supplierdim"

val deltapathlocation = bloburl + "/deltaidi/delta_locationdim"

val deltapathteam = bloburl + "/deltaidi/delta_teamdim"

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

val checkpointLocation = bloburl + "/deltaidi/delta_custdata/_checkpoints/etl-from-json"
val deltapath = bloburl + "/deltaidi/delta_custdata"
val checkpointLocationforcosmo1 = bloburl + "/deltaidi/delta_custdata/_checkpoints/etl-from-json_cosmos1"

// COMMAND ----------

dbutils.fs.rm(bloburl+"/deltaidi/delta_custdata/_checkpoints/etl-from-json_cosmos1", true)

// COMMAND ----------

val messages = spark.readStream.format("delta").option("ignoreDeletes", "true").option("ignoreChanges", "true").option("path", deltapath).table("delta_custdata")

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._

// COMMAND ----------

val writeConfig = Config(Map(
  "Endpoint" -> scmcosmosuri,
  "Masterkey" -> scmcosmoskey,
  "Database" -> scmcosmosdb,
  //"Collection" -> scmcosmoscontainer,
  "Collection" -> "custdata3",
  "Upsert" -> "true"
))

messages
        .join(dfproduct, "customername")
        .join(dfsupplier, "customername")
        .join(dflocation, "customername")
        .join(dfteam, "customername")
        .withColumn("id", col("productname"))
        .select("id","eventdatetime","customername","address","city","state","zip","productname","suppliername","locationname","teamname")
        .writeStream
        //.option("checkpointLocation", checkpointLocationforcosmo1)
        .foreachBatch((messages: DataFrame, batchId: Long) => {
           //val startTimestamp = currentTimestamp();
           //logStart(startTimestamp); // <- Function to track batch start 
           print("Batch id" + batchId)
           messages.write.mode(SaveMode.Overwrite).cosmosDB(writeConfig)
 
           val count = messages.count();
           //logEnd(startTimestamp, count); // <- Function to track batch end
          //display(count)
          print(count)
        })
        .start();


// COMMAND ----------

val ConfigMap = Map(
"Endpoint" -> scmcosmosuri,
"Masterkey" -> scmcosmoskey,
"Database" -> scmcosmosdb,
//"Collection" -> scmcosmoscontainer,
"Collection" -> "custdata3",
"Upsert" -> "true"
)

messages
        .join(dfproduct, "customername")
        .join(dfsupplier, "customername")
        .join(dflocation, "customername")
        .join(dfteam, "customername")
        .withColumn("id", col("productname"))
        .select("id","eventdatetime","customername","address","city","state","zip","productname","suppliername","locationname","teamname")
        .writeStream
        //.option("checkpointLocation", checkpointLocationforcosmo1)
        .foreachBatch((messages: DataFrame, batchId: Long) => {
           //val startTimestamp = currentTimestamp();
           //logStart(startTimestamp); // <- Function to track batch start 
           print('Batch id' + batchId)
           messages.write
                  .format(classOf[CosmosDBSinkProvider].getName) // <- Using CosmosDBSinkProvider gives an error (*)
                  .options(ConfigMap)
                  .mode(SaveMode.Overwrite) // <- outputMode is not defined in DataFrameWriter
                  .partitionBy("locationPartition")
                  .save();
     
           val count = messages.count();
           //logEnd(startTimestamp, count); // <- Function to track batch end
        })
        .start();
