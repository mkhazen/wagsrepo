# Loading Data into Azure synapse analytics using structured stream in parallel streams

## End to End Architecture

![alt text](https://github.com/balakreshnan/wagsrepo/blob/master/images/idiserversynapsestream.jpg "Architecture")

## setup Azure synpase analytics

Create a user name to use for ingest

```
  Use Master;

  CREATE LOGIN adbingest WITH PASSWORD = 'xxxxxxxx';

  -- execute in the database
  CREATE USER adbingest FROM LOGIN adbingest; 

  ALTER ROLE dbmanager ADD MEMBER adbingest; 

  GRANT ALTER ANY USER TO adbingest;

  ALTER ROLE db_owner ADD MEMBER adbingest; 

  --worked
  EXEC sp_addrolemember 'db_owner', 'adbingest';
```

Check the resource group
```
SELECT name
FROM   sys.database_principals
WHERE  name LIKE '%rc%' AND type_desc = 'DATABASE_ROLE';

EXEC sp_addrolemember 'xlargerc', 'adbingest';

to start with Static10rc should be enough.

EXEC sp_droprolemember 'xlargerc', 'adbingest';
```

Setup workload isolation

```
CREATE WORKLOAD GROUP adbload 
WITH
  ( MIN_PERCENTAGE_RESOURCE = 50                -- integer value
    , REQUEST_MIN_RESOURCE_GRANT_PERCENT = 25 -- factor of 26 (guaranteed a minimum of 8 concurrency)
    , CAP_PERCENTAGE_RESOURCE = 100 )

CREATE WORKLOAD CLASSIFIER adbstreamload WITH  
( WORKLOAD_GROUP = 'adbserve'
 ,MEMBERNAME     = 'adbingest'  
 ,WLM_LABEL      = 'facts_loads' )

SELECT COUNT(*) 
  FROM custdata1
  OPTION (LABEL = 'facts_loads')

select count(*) from custdata2;
```

Create DDL for tables load

Create 10 tables to insert simulatneously

```
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

Drop table [dbo].[custdata10]

CREATE TABLE [dbo].[custdata10]
(
	[eventdatetime] [varchar](400) NULL,
	[customername] [varchar](300) NULL,
	[address] [varchar](500) NULL,
	[city] [varchar](100) NULL,
	[state] [varchar](50) NULL,
	[zip] [varchar](50) NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	CLUSTERED COLUMNSTORE INDEX
)
GO

Drop table [dbo].[custdata11]

CREATE TABLE [dbo].[custdata11]
(
	[eventdatetime] [varchar](400) NULL,
	[customername] [varchar](300) NULL,
	[address] [varchar](500) NULL,
	[city] [varchar](100) NULL,
	[state] [varchar](50) NULL,
	[zip] [varchar](50) NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	CLUSTERED COLUMNSTORE INDEX
)
GO

Drop table [dbo].[custdata12]

CREATE TABLE [dbo].[custdata12]
(
	[eventdatetime] [varchar](400) NULL,
	[customername] [varchar](300) NULL,
	[address] [varchar](500) NULL,
	[city] [varchar](100) NULL,
	[state] [varchar](50) NULL,
	[zip] [varchar](50) NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	CLUSTERED COLUMNSTORE INDEX
)
GO

Drop table [dbo].[custdata13]

CREATE TABLE [dbo].[custdata13]
(
	[eventdatetime] [varchar](400) NULL,
	[customername] [varchar](300) NULL,
	[address] [varchar](500) NULL,
	[city] [varchar](100) NULL,
	[state] [varchar](50) NULL,
	[zip] [varchar](50) NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	CLUSTERED COLUMNSTORE INDEX
)
GO

Drop table [dbo].[custdata14]

CREATE TABLE [dbo].[custdata14]
(
	[eventdatetime] [varchar](400) NULL,
	[customername] [varchar](300) NULL,
	[address] [varchar](500) NULL,
	[city] [varchar](100) NULL,
	[state] [varchar](50) NULL,
	[zip] [varchar](50) NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	CLUSTERED COLUMNSTORE INDEX
)
GO

Drop table [dbo].[custdata15]

CREATE TABLE [dbo].[custdata15]
(
	[eventdatetime] [varchar](400) NULL,
	[customername] [varchar](300) NULL,
	[address] [varchar](500) NULL,
	[city] [varchar](100) NULL,
	[state] [varchar](50) NULL,
	[zip] [varchar](50) NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	CLUSTERED COLUMNSTORE INDEX
)
GO

Drop table [dbo].[custdata16]

CREATE TABLE [dbo].[custdata16]
(
	[eventdatetime] [varchar](400) NULL,
	[customername] [varchar](300) NULL,
	[address] [varchar](500) NULL,
	[city] [varchar](100) NULL,
	[state] [varchar](50) NULL,
	[zip] [varchar](50) NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	CLUSTERED COLUMNSTORE INDEX
)
GO

Drop table [dbo].[custdata17]

CREATE TABLE [dbo].[custdata17]
(
	[eventdatetime] [varchar](400) NULL,
	[customername] [varchar](300) NULL,
	[address] [varchar](500) NULL,
	[city] [varchar](100) NULL,
	[state] [varchar](50) NULL,
	[zip] [varchar](50) NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	CLUSTERED COLUMNSTORE INDEX
)
GO

Drop table [dbo].[custdata18]

CREATE TABLE [dbo].[custdata18]
(
	[eventdatetime] [varchar](400) NULL,
	[customername] [varchar](300) NULL,
	[address] [varchar](500) NULL,
	[city] [varchar](100) NULL,
	[state] [varchar](50) NULL,
	[zip] [varchar](50) NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	CLUSTERED COLUMNSTORE INDEX
)
GO

Drop table [dbo].[custdata19]

CREATE TABLE [dbo].[custdata19]
(
	[eventdatetime] [varchar](400) NULL,
	[customername] [varchar](300) NULL,
	[address] [varchar](500) NULL,
	[city] [varchar](100) NULL,
	[state] [varchar](50) NULL,
	[zip] [varchar](50) NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	CLUSTERED COLUMNSTORE INDEX
)
GO
```

Clear the tables

```
truncate table custdata10;
truncate table custdata11;
truncate table custdata12;
truncate table custdata13;
truncate table custdata14;
truncate table custdata15;
truncate table custdata16;
truncate table custdata17;
truncate table custdata18;
truncate table custdata19;
```


## Structured streaming implementation for parallel

```
Class.forName("com.databricks.spark.sqldw.DefaultSource")
```

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
import org.apache.spark.sql.SaveMode

```

Assign variable to hold the suffix.

```
dbutils.widgets.text("tableid", "10")
val tableid = dbutils.widgets.get("tableid")
```

```
print(tableid)
```

Set the ADLS configuration

```
spark.conf.set(   "fs.azure.account.key.xxxx.blob.core.windows.net", "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
```

Configure the variables for deltapath and check point location

```
val checkpointLocation = "wasbs://deltaidi@waginput.blob.core.windows.net/deltaidi/delta_custdata" + tableid + "/_checkpoints/etl-from-json"
val deltapath = "wasbs://deltaidi@waginput.blob.core.windows.net/deltaidi/delta_custdata"
val checkpointLocationforsqldw = "wasbs://deltaidi@waginput.blob.core.windows.net/deltaidi/delta_custdata" + tableid + "/_checkpoints/etl-from-json_sqldw1"
```

Create the jdbc string to be used for Azure Synapse Analytics

```
val jdbcconn = "jdbc:sqlserver://idicdmsvr.database.windows.net:1433;database=idicdm;user=xxxxx@idicdmsvr;password=xxxxxxx;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
```

Clear the checkpoint to reprocess for testing only.

```
dbutils.fs.rm("wasbs://xxxxx@xxxxxx.blob.core.windows.net/deltaidi/delta_custdata"+ tableid +"/_checkpoints/etl-from-json_sqldw1", true)
```

Now time to read the delta log stream.

```
val messages = spark.readStream.format("delta").option("ignoreDeletes", "true").option("ignoreChanges", "true").option("path", deltapath).table("delta_custdata").select("eventdatetime","customername","address","city","state","zip")
```

Wite back to Azyre Synapse Analytics.

```
val tablename = "custdata" + tableid
messages.writeStream
  .format("com.databricks.spark.sqldw")
  .option("url", jdbcconn)
  .option("tempDir", "wasbs://cccc@xxxxxxx.blob.core.windows.net/stage")
  .option("forwardSparkAzureStorageCredentials", "true")
  .option("dbTable", tablename)
  .option("checkpointLocation", checkpointLocationforsqldw)
  .start()
```

## Use Azure Data factory to run 10 streaming jobs simulanteously.

## Pipeline Architecture

![alt text](https://github.com/balakreshnan/wagsrepo/blob/master/images/adfstreaming1.jpg "Architecture")

We are going to call the same notebook but passing tableid as parameter variable ex tableid10, tableid11 etc

Configure the ADB connection using token. Then select the notebook.
Now go to settings and add new base parameters
tableid = 10
tableid = 11
tableid = 12
tableid = 13
tableid = 14
tableid = 15
tableid = 16
tableid = 17
tableid = 18
tableid = 19

Now run the streaming job.