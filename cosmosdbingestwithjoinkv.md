# Event based Delta lake and loading real time cache such as Azure cosmos db

Provide a architecture that can get real time events and gets processed into delta lake for long term storage using structured streaming. Delta lake provides ability to do change data capture.

The reason why we are choosing to load data into Azure cosmos db is to tackle real time queries from various API that interface to various other business application and also to provide close to real time data as possible.

Stream reads the event expands it and splits into various schema or tables. Then another stream is responsible for reading a combination of tables and pick columns that are needed and push into cosmos db as they arrive.

Goal: to provide guidance only. Below articles assumes lot of fake data and less business logic processing. Provides a guidance on how to create a event driven delta lake for ingestion, curation and serve layer. 

Assumption: Keeping long term storage in delta lake and using Cosmos for Serve layer as one option. Only choose this option if there is use case where high concurrency and data delviered through API to other systems. Not meant to do analytical work load.

## Architecture

![alt text](https://github.com/balakreshnan/wagsrepo/blob/master/images/scmcosmos1.jpg "Architecture")

## Data Simulator

I created a data simulator which sends out JSON message as 
{"eventdatetime":"2020-02-24T12:57:37.6956159-06:00","customername":"idicustomer 0","address":"xyz street","city":"new york","zip":"11022","Date": "2020-02-24"}

The app was wrote in c# to generate data and send on demand basis to event hub

https://github.com/balakreshnan/wagsrepo/blob/master/datagen.md

## Event hub

Event hub namespace created. Create a event hub with default 4 parition for now.

## Azure Databricks

## Azure databricks Cluster configuration

Cluster mode: Standard
Python: 3
Databricks RUntime version 6.3

Worker Type: Standard DS4_V2
Driver Type: Standard DS4_V2

Minimum nodes: 5
Maximum nodes 10
Enable Autoscaling: Checked
Terminate after: 20 minutes

Libraries:
Cosmos DB: azure_cosmosdb_spark_2_4_0_2_11_1_4_0_uber.jar
Event Hub: com.microsoft.azure:azure-eventhubs-spark_2.11:2.3.12
Azure SQL Db: com.microsoft.azure:azure-sqldb-spark:1.0.2
Azure SQL DW: already loaded in cluster


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

Configuration for ADLS Access
```
spark.conf.set(   "fs.azure.account.key.xxxxx.blob.core.windows.net", "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
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
           get_json_object(($"body").cast("string"), "$.state").alias("state"),
           get_json_object(($"body").cast("string"), "$.zip").alias("zip"),
           get_json_object(($"body").cast("string"), "$.eventdatetime").cast("date").alias("Date")
          )

messages.printSchema
```

```
val checkpointLocation = "wasbs://xxxxxx@xxxxxx.blob.core.windows.net/deltaidi/delta_custdata/_checkpoints/etl-from-json"
val deltapath = "wasbs://xxxxxxx@xxxxxxx.blob.core.windows.net/deltaidi/delta_custdata"
```

Now run the stream. This would be a continously running process.
```
messages.writeStream
  .outputMode("append")
  .option("checkpointLocation", checkpointLocation)
  .option("mergeSchema", "true")
  .format("delta")
  .partitionBy("Date")
  .option("path", deltapath)
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

## Now Part 2  Create a data load to read from delta lake and push to cosmos db using key vault

Now we are going to push data from taking the delta changes from delta lake in structured streaming.
Load all the dimension as static data frame and then join and select only necessary columns

Sample dimenstion tables are available in code/sampledata folder

Prerequistie: upload the sample data into ADLS store before you begin this section.
Also configure the key vault store and set the scope as wagsdata

Below is the sample how secrets are saved in azure key vault

![alt text](https://github.com/balakreshnan/wagsrepo/blob/master/images/cosmoskvconfig.jpg "Key Vault")

here is now to load the data into delta lake:

https://github.com/balakreshnan/wagsrepo/blob/master/deltadataload.md

Lets add all the includes

```
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
```

```
val blobname = dbutils.secrets.get(scope = "wagsdata", key = "blobname")
val blobkey = dbutils.secrets.get(scope = "wagsdata", key = "blobkey")
val scmcosmosuri = dbutils.secrets.get(scope = "wagsdata", key = "scmcosmosuri")
val scmcosmoskey = dbutils.secrets.get(scope = "wagsdata", key = "scmcosmoskey")
val scmcosmosdb = dbutils.secrets.get(scope = "wagsdata", key = "scmcosmosdb")
val scmcosmoscontainer = dbutils.secrets.get(scope = "wagsdata", key = "scmcosmoscontainer")
val blobcontainer = dbutils.secrets.get(scope = "wagsdata", key = "blobcontainer")
```

```
spark.conf.set(   "fs.azure.account.key."+ blobname +".blob.core.windows.net", blobkey)
```

```
val bloburl = "wasbs://"+blobcontainer+"@"+blobname+".blob.core.windows.net"
```

Set the dimension path:
```
val deltapathproduct = bloburl + "/deltaidi/delta_productdim"

val deltapathsupplier = bloburl + "/deltaidi/delta_supplierdim"

val deltapathlocation = bloburl + "/deltaidi/delta_locationdim"

val deltapathteam = bloburl + "/deltaidi/delta_teamdim"
```

Now load the data for dimension before we start the stream. 
Note: make sure no duplicates column name

```
//load dimensions now
val dfproduct = spark.read.format("delta").load(deltapathproduct).select("customername","productname")
//load dimensions now
val dfsupplier = spark.read.format("delta").load(deltapathsupplier).select("customername","suppliername")
val dflocation = spark.read.format("delta").load(deltapathlocation).select("customername","locationname")
val dfteam = spark.read.format("delta").load(deltapathteam).select("customername","teamname")
```

```
val checkpointLocation = bloburl + "/deltaidi/delta_custdata/_checkpoints/etl-from-json"
val deltapath = bloburl + "/deltaidi/delta_custdata"
val checkpointLocationforcosmo1 = bloburl + "/deltaidi/delta_custdata/_checkpoints/etl-from-json_cosmos1
```


to reprocess the data again make sure delete the checkpoint folder
```
dbutils.fs.rm(bloburl+"/deltaidi/delta_custdata/_checkpoints/etl-from-json_cosmos1", true)
```

Now load the delta table changes into stream
```
val messages = spark.readStream.format("delta").option("ignoreDeletes", "true").option("ignoreChanges", "true").option("path", deltapath).table("delta_custdata")
```

now to move those changes to cosmos db using structured streaming

Note: i am using customername as the key to join all the tables.

to update unique values as upsert 
1) Create a parition key in my case was customername
2) create a unique key as productname in my case here

Combination of customername and productname becomes the unique to update the row.
Update in cosmos is replacing the whole row.

```
val ConfigMap = Map(
"Endpoint" -> scmcosmosuri,
"Masterkey" -> scmcosmoskey,
"Database" -> scmcosmosdb,
"Collection" -> scmcosmoscontainer,
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
  .format(classOf[CosmosDBSinkProvider].getName)
  .outputMode("update")
  .options(ConfigMap)
  .option("checkpointLocation", checkpointLocationforcosmo1)
  .start()
  ```

  Now data will be flowing to cosmos db.

  Go to Cosmos DB resource in Portal or use cosmos.azure.com and login into your collection and validate and see if the data is flowing or not.

  When i tested i was able to see the data flowing. 

  Note: Remember this code doesn't have any business logic as all the data are fake. In real work scenario i am sure there would be lot more business logic processing.