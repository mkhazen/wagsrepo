// Databricks notebook source
    import scala.collection.JavaConverters._
    import com.microsoft.azure.eventhubs._
    import java.util.concurrent._
    import scala.collection.immutable._
    import scala.concurrent.Future
    import scala.concurrent.ExecutionContext.Implicits.global

    val namespaceName = "IDIincoming"
    val eventHubName = "evetincoming"
    val sasKeyName = "rw"
    val sasKey = "KLrDLpiZ90K7MKRr07CBV15r5Lpdpq5C1a3BQg6iiZ4="
    val connStr = new ConnectionStringBuilder()
                .setNamespaceName(namespaceName)
                .setEventHubName(eventHubName)
                .setSasKeyName(sasKeyName)
                .setSasKey(sasKey)


// COMMAND ----------

val pool = Executors.newScheduledThreadPool(1)
val eventHubClient = EventHubClient.create(connStr.toString(), pool)

// COMMAND ----------

import org.apache.spark.eventhubs._
import com.microsoft.azure.eventhubs._

// COMMAND ----------

val customEventhubParameters =
      EventHubsConf(connStr.toString())
      .setMaxEventsPerTrigger(5)

// COMMAND ----------

val incomingStream = spark.readStream.format("eventhubs").options(customEventhubParameters.toMap).load()
incomingStream.printSchema

// COMMAND ----------

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

// COMMAND ----------

import org.apache.spark.sql.functions._

// COMMAND ----------

val messages =
   incomingStream
   .withColumn("Offset", $"offset".cast(LongType))
   .withColumn("Time (readable)", $"enqueuedTime".cast(TimestampType))
   .withColumn("Timestamp", $"enqueuedTime".cast(LongType))
   .withColumn("Body", $"body".cast(StringType))
   .select(get_json_object(($"body").cast("string"), "$.eventdatetime").alias("eventdatetime"),
           get_json_object(($"body").cast("string"), "$.customername").alias("customername"),
           get_json_object(($"body").cast("string"), "$.address").alias("address"),
           get_json_object(($"body").cast("string"), "$.city").alias("city"),
           get_json_object(($"body").cast("string"), "$.zip").alias("zip")
          )
   //.select("Offset", "Time (readable)", "Timestamp", "Body")

messages.printSchema

// COMMAND ----------

// MAGIC %sql
// MAGIC --Drop table custdata;
// MAGIC Drop table delta_custdata;

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE TABLE custdata (eventdatetime STRING, customername STRING,address STRING,city STRING  , zip STRING, Date date)

// COMMAND ----------

messages.createOrReplaceTempView("custdata")

// COMMAND ----------

// MAGIC %sql
// MAGIC TRUNCATE TABLE delta_custdata

// COMMAND ----------

import org.apache.spark.sql.SaveMode


// COMMAND ----------

val df = messages.withColumn("Date", (col("eventdatetime").cast("date"))) 
//display(df1)

// COMMAND ----------

messages.printSchema()

// COMMAND ----------

df.printSchema()

// COMMAND ----------

df.writeStream
  .outputMode("append")
  .option("checkpointLocation", "/delta/events/_checkpoints/etl-from-json")
  .option("mergeSchema", "true")
  .format("delta")
  .partitionBy("Date")
  .table("delta_custdata")
  //.save("/delta/custdata")

// COMMAND ----------

//write text data out to console
messages.writeStream.outputMode("append").format("console").option("truncate", false).start().awaitTermination()

// COMMAND ----------

df.createOrReplaceTempView("device_telemetry_data")

// COMMAND ----------



// COMMAND ----------

// Sending the incoming stream into the console.
// Data comes in batches!
//binary display
incomingStream.writeStream.outputMode("append").format("console").option("truncate", false).start().awaitTermination()

// COMMAND ----------

val custdatadf = spark.table("delta_custdata")

// COMMAND ----------

display(custdatadf)

// COMMAND ----------

custdatadf.count

// COMMAND ----------

