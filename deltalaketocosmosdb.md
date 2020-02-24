# Event based Delta lake and loading real time cache using cosmos db

Provide a architecture that can get real time events and gets processed into delta lake for long term storage using structured streaming. Delta lake provides ability to do change data capture.

Stream reads the event expands it and splits into various schema or tables. Then another stream is responsible for reading a combination of tables and pick columns that are needed and push into cosmos db as they arrive.

## Architecture

![alt text](https://github.com/balakreshnan/wagsrepo/blob/master/images/scmcosmos.jpg "Architecture")

## Data Simulator

I created a data simulator which sends out JSON message as 
{"eventdatetime":"2020-02-24T12:57:37.6956159-06:00","customername":"idicustomer 0","address":"xyz street","city":"new york","zip":"11022","Date": "2020-02-24"}

The app was wrote in c# to generate data and send on demand basis to event hub

## Event hub

Event hub namespace created. Create a event hub with default 4 parition for now.

## Azure Databricks

Clear any data in Delta table table
```
%sql
Drop table delta_custdata;
```

Note to clear all data in delta table
```
%sql
TRUNCATE TABLE delta_custdata
```

Now setup the Structured streaming job

Import the necessary libraries to use.
```
    import scala.collection.JavaConverters._
    import com.microsoft.azure.eventhubs._
    import java.util.concurrent._
    import scala.collection.immutable._
    import scala.concurrent.Future
    import scala.concurrent.ExecutionContext.Implicits.global
    import org.apache.spark.eventhubs._
    import com.microsoft.azure.eventhubs._
    import org.apache.spark.sql.types._
    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.SaveMode

    val namespaceName = "eventincoming"
    val eventHubName = "evetincoming"
    val sasKeyName = "rw"
    val sasKey = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
    val connStr = new ConnectionStringBuilder()
                .setNamespaceName(namespaceName)
                .setEventHubName(eventHubName)
                .setSasKeyName(sasKeyName)
                .setSasKey(sasKey)
```

```
val pool = Executors.newScheduledThreadPool(1)
val eventHubClient = EventHubClient.create(connStr.toString(), pool)
```

Setup the event hub connection string and wait for 5 events and then process basically micro batching every 5 events.
```
val customEventhubParameters =
      EventHubsConf(connStr.toString())
      .setMaxEventsPerTrigger(5)
```

Setting up the stream
```
val incomingStream = spark.readStream.format("eventhubs").options(customEventhubParameters.toMap).load()
incomingStream.printSchema
```

Configure the stream to split the schema message properties. I am also adding a date property to use as parition key.
```
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
           get_json_object(($"body").cast("string"), "$.zip").alias("zip"),
           get_json_object(($"body").cast("string"), "$.eventdatetime").cast("date").alias("Date")
          )

messages.printSchema
```

Now run the stream. This would be a continously running process.
```
messages.writeStream
  .outputMode("append")
  .option("checkpointLocation", "/delta/events/_checkpoints/etl-from-json")
  .option("mergeSchema", "true")
  .format("delta")
  .partitionBy("Date")
  .table("delta_custdata")
```
If you want to view the streaming statistics expand the arrow on the bottom status and should display how many rows are getting processed.

To check the delta lake 
```
val custdatadf = spark.table("delta_custdata")
```

Display the dataset to see if data is shown
```
display(custdatadf)
```

Count the dataset to see if we can validate how much data was pushed to delta lake
```
custdatadf.count
```

Now Part 2  Create a data load to read from delta lake and push to cosmos db

```
Note: make sure cosmos db uber jar is uploaded as library to the cluster. Make sure the version matches the scala version and spark version
```

```
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
// Import Necessary Libraries
import com.microsoft.azure.cosmosdb.spark.schema._
import com.microsoft.azure.cosmosdb.spark._
import com.microsoft.azure.cosmosdb.spark.config.Config
```

```
val custdatadf = spark.table("delta_custdata")
```

```
display(custdatadf)
```

Sample Read from cosmos db
```
// Read Configuration
val readConfig = Config(Map(
  "Endpoint" -> "https://xxxxx.documents.azure.com:443/",
  "Masterkey" -> "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "Database" -> "dbtest",
  "Collection" -> "containername",
  "query_custom" -> "SELECT * from c" // Optional
))

// Connect via azure-cosmosdb-spark to create Spark DataFrame
val itemdf = spark.read.cosmosDB(readConfig)
itemdf.count()
```

Now time to write the data back into cosmos db.
```
// Write configuration
val writeConfig = Config(Map(
  "Endpoint" -> "https://xxxxx.documents.azure.com:443/",
  "Masterkey" -> "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "Database" -> "dbtest",
  "Collection" -> "containername",
  "WritingBatchSize" -> "5"
))

// Write to Cosmos DB from the flights DataFrame
import org.apache.spark.sql.SaveMode
custdatadf.repartition(1).write.mode(SaveMode.Overwrite).cosmosDB(writeConfig)
```
