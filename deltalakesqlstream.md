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

Now it is time to implement structured streaming to load the data into azure synapse analytics

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

Notebook Name: synapseingeststreamtest

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

Load the secrets from key vault.

```
val blobname = dbutils.secrets.get(scope = "wagsdata", key = "blobname")
val blobkey = dbutils.secrets.get(scope = "wagsdata", key = "blobkey")
val scmcosmoscontainer = dbutils.secrets.get(scope = "wagsdata", key = "scmcosmoscontainer")
val blobcontainer = dbutils.secrets.get(scope = "wagsdata", key = "blobcontainer")

val sqldwname1 = dbutils.secrets.get(scope = "wagsdata", key = "sqldwname1")
val sqldwuser1 = dbutils.secrets.get(scope = "wagsdata", key = "sqldwuser1")
val sqldwpwd1 = dbutils.secrets.get(scope = "wagsdata", key = "sqldwpwd1")
val sqldwsvrname1 = dbutils.secrets.get(scope = "wagsdata", key = "sqldwsvrname1")
val sqldwdatabase1 = dbutils.secrets.get(scope = "wagsdata", key = "sqldwdatabase1")
```

Set the ADLS configuration

```
spark.conf.set(   "fs.azure.account.key."+ blobname +".blob.core.windows.net", blobkey)
```

```
val bloburl = "wasbs://"+blobcontainer+"@"+blobname+".blob.core.windows.net"
```

Configure the variables for deltapath and check point location

```
val checkpointLocation = bloburl + "/deltaidi/delta_custdata" + tableid + "/_checkpoints/etl-from-json"
//val deltapath = "wasbs://deltaidi@waginput.blob.core.windows.net/deltaidi/delta_custdata" + tableid
val deltapath = bloburl + "/deltaidi/delta_custdata"
val checkpointLocationforsqldw = bloburl + "/deltaidi/delta_custdata" + tableid + "/_checkpoints/etl-from-json_sqldw1"
```

Create the jdbc string to be used for Azure Synapse Analytics

```
val jdbcconn = "jdbc:sqlserver://" + sqldwname1 + ":1433;database=" + sqldwdatabase1 + ";user=" + sqldwuser1 + "@" + sqldwsvrname1 + ";password=" + sqldwpwd1 + ";encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
```

Clear the checkpoint to reprocess for testing only.

```
dbutils.fs.rm(bloburl + "/deltaidi/delta_custdata"+ tableid +"/_checkpoints/etl-from-json_sqldw1", true)
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
tableid = 10 <br />
tableid = 11 <br />
tableid = 12 <br />
tableid = 13 <br />
tableid = 14 <br />
tableid = 15 <br />
tableid = 16 <br />
tableid = 17 <br />
tableid = 18 <br />
tableid = 19 <br />

Now run the streaming job.

## Workload management using resource class in Azure synapse analytics.

The goal here is try to find the right resoruce class to use for all streams using one login but using different sessions.
Each stream will same user name but with different session.

Every run with time it took are recorded. TO find the exact for your use case please run similiar test like this in your production like environment and test it and see.

Statics on running

Azure Synapse Analytics: Gen2: DW100c

```
EXEC sp_addrolemember 'xlargerc', 'adbingest';
```

Time Taken: 1:05 mins

```
EXEC sp_addrolemember 'staticrc20', 'adbingest';
```

Time Taken: 4:10 mins

```
EXEC sp_addrolemember 'staticrc40', 'adbingest';
```

Time Taken: 2:10 mins

```
EXEC sp_addrolemember 'staticrc50', 'adbingest';
```

Time Taken: 1:05 mins

## Increase the DW units a little and try the above combination.

Azure Synapse Analytics: Gen2: DW200c

```
EXEC sp_addrolemember 'staticrc50', 'adbingest';
```

Time Taken: 1:05 mins

```
EXEC sp_addrolemember 'staticrc20', 'adbingest';
```

Time Taken: 1:10 mins

## create a database script to create 100 or so tables

The above test was only performed with 10 tables but we need to increase the count to 100 or so and test.

The below script basically creates tables by specificing from what number to start and how many tables are needed.

Log into SQL management studio and create a stored procedure as below

```
-- ======================================================================
-- Create Stored Procedure Template for Azure SQL Data Warehouse Database
-- ======================================================================
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:      <Author, , Name>
-- Create Date: <Create Date, , >
-- Description: <Description, , >
-- =============================================
Create PROCEDURE usp_createtable
(
    -- Add the parameters for the stored procedure here
    @start int,
	@end int
)
AS
BEGIN
    -- SET NOCOUNT ON added to prevent extra result sets from
    -- interfering with SELECT statements.
    SET NOCOUNT ON

    -- Insert statements for procedure here
    --SELECT <@Param1, sysname, @p1>, <@Param2, sysname, @p2>

	Declare @endcount int

	Set @endcount = @start + @end;

	WHILE   @start < @endcount
	BEGIN
		DECLARE @tablename varchar(400)
		
		Set @tablename = 'custdata' + cast(@start AS VARCHAR(10))
		print @tablename

		

		DECLARE @sql_code NVARCHAR(4000) = 'IF OBJECT_ID(N'''+@tablename+''', N''U'') IS NOT NULL Drop table ' + @tablename + ' ; CREATE TABLE ' + @tablename + ' (	[eventdatetime] [varchar](400) NULL,	[customername] [varchar](300) NULL,	[address] [varchar](500) NULL,	[city] [varchar](100) NULL,	[state] [varchar](50) NULL,	[zip] [varchar](50) NULL);';
		--print @sql_code
		EXEC    sp_executesql @sql_code;
		
		SET     @start +=1;
	END

END
GO

```
TO delete the tables:

```
/****** Object:  StoredProcedure [dbo].[usp_createtable]    Script Date: 3/4/2020 9:23:17 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER PROC [dbo].[usp_createtable] @start [int],@end [int] AS
BEGIN
    -- SET NOCOUNT ON added to prevent extra result sets from
    -- interfering with SELECT statements.
    SET NOCOUNT ON

    -- Insert statements for procedure here
    --SELECT <@Param1, sysname, @p1>, <@Param2, sysname, @p2>

	Declare @endcount int

	Set @endcount = @start + @end;

	WHILE   @start < @endcount
	BEGIN
		DECLARE @tablename varchar(400)
		
		Set @tablename = 'custdata' + cast(@start AS VARCHAR(10))
		print @tablename

		

		DECLARE @sql_code NVARCHAR(4000) = 'IF OBJECT_ID(N'''+@tablename+''', N''U'') IS NOT NULL Drop table ' + @tablename + ' ; CREATE TABLE ' + @tablename + ' (	[eventdatetime] [varchar](400) NULL,	[customername] [varchar](300) NULL,	[address] [varchar](500) NULL,	[city] [varchar](100) NULL,	[state] [varchar](50) NULL,	[zip] [varchar](50) NULL);';
		--print @sql_code
		EXEC    sp_executesql @sql_code;
		
		SET     @start +=1;
	END

END
```

then create 100 tables try this is SSMS

First parameter is number to start from in my case i am starting from 20 <br />
Second parameter is number of table to create in my case it is 100 <br />
So table will start from custdata20 and will end custdata119

```
exec usp_createtable 20,100
```

## Create a Spark Scala notebook to run multiple streams

Since we need to test like 100's of stream doing that manual is challenging.
So writing a scala notebook to loop a list which can be parallized and then run the notebook to stream data.

Create 2 more notebooks

Notebook 1 name: Invokestreams
Notebook 2 name: runstreams

Code for Invokestreams

First build a parallel list so that we can parallize the notebook runs

```
val list = (20 to 29).toList
list.par.map(_ + 0)
```

Now Execute the notebook by passing the table id as parameter.

```
// define some way to generate a sequence of workloads to run
val jobArguments = list
 
// define the name of the Azure Databricks notebook to run
val notebookToRun = "runstreams"

// look up required context for parallel run calls
val context = dbutils.notebook.getContext()
 
// start the jobs
list.par.foreach(args => {
  // ensure thread knows about databricks context
  dbutils.notebook.setContext(context)
  //dbutils.notebook.run(notebookToRun, timeoutSeconds = 0, args.toString)
  dbutils.notebook.run(notebookToRun, timeoutSeconds = 0, Map("tableid" -> args.toString()))
})
```

Code for runstreams

Assign default value

```
dbutils.widgets.text("tableid", "10")
```

Now read the parameters passed to notebook

```
val tableid = dbutils.widgets.get("tableid")
```

Print the parameter variable

```
print(tableid)
```

Now create a vairable for notebook to run and assin the name of notebook as : synapseingeststreamtest

```
val notebookToRun = "synapseingeststreamtest"
```

Now time to run the notebook and pass the parameter or argument

```
dbutils.notebook.run(notebookToRun, timeoutSeconds = 0, Map("tableid" -> tableid))
```

```
dbutils.notebook.exit(tableid)
```

Once you run the notebook you should see 8 notebooks run parallel.
If cancelled the main one only the immediate child notebook cancels other wise it doesn't cancel the child-child.

Now going to run parent child combination:

Code for Invokestreams

First build a parallel list so that we can parallize the notebook runs
Depending on how many tables or streams to create we can increase the number in the list.
Make sure there are enough tables precreated for the test to run.

```
val list = (20 to 29).toList
list.par.map(_ + 0)
```

Now Execute the notebook by passing the table id as parameter.

```
// define some way to generate a sequence of workloads to run
val jobArguments = list
 
// define the name of the Azure Databricks notebook to run
val notebookToRun = "synapseingeststreamtest"

// look up required context for parallel run calls
val context = dbutils.notebook.getContext()
 
// start the jobs
list.par.foreach(args => {
  // ensure thread knows about databricks context
  dbutils.notebook.setContext(context)
  //dbutils.notebook.run(notebookToRun, timeoutSeconds = 0, args.toString)
  dbutils.notebook.run(notebookToRun, timeoutSeconds = 0, Map("tableid" -> args.toString()))
})
```

Now run the Invokestreams but this time call the actual stream that writes to Azure Synapse Analytics.
Only 8 parallel runs are running.

https://www.microsoft.com/developerblog/2019/01/18/running-parallel-apache-spark-notebook-workloads-on-azure-databricks/

Now change the parallel code to more execute more jobs

```
val jobArguments = ""
 
// define the name of the Azure Databricks notebook to run
val notebookToRun = "synapseingeststreamtest"
 
// define maximum number of jobs to run in parallel
val totalJobs = 10
 
import java.util.concurrent.Executors
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.Duration
 


// look up required context for parallel run calls
val context = dbutils.notebook.getContext()
 
// create threadpool for parallel runs
implicit val executionContext = ExecutionContext.fromExecutorService(
  Executors.newFixedThreadPool(totalJobs))
 
try {
  val futures = list.zipWithIndex.map { case (args, i) =>
    Future({
      // ensure thread knows about databricks context
      dbutils.notebook.setContext(context)
 
      // define up to maxJobs separate scheduler pools
      sc.setLocalProperty("spark.scheduler.pool", s"pool${i % totalJobs}")
 
      // start the job in the scheduler pool
      dbutils.notebook.run(notebookToRun, timeoutSeconds = 0, Map("tableid" -> args.toString())
    })}
 
  // wait for all the jobs to finish processing
  Await.result(Future.sequence(futures), atMost = Duration.Inf)
} finally {
  // ensure to clean up the threadpool
  executionContext.shutdownNow()
}
```

To clean up databricks streaming tables:

```
exec usp_cleanDatabrickStreamingtables;
```
Cleaning up databricks streaming temporary tables.

```
-- ======================================================================
-- Create Stored Procedure Template for Azure SQL Data Warehouse Database
-- ======================================================================
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE usp_cleanDatabrickStreamingtables
AS
BEGIN
    -- SET NOCOUNT ON added to prevent extra result sets from
    -- interfering with SELECT statements.
    SET NOCOUNT ON

drop table #tbl
CREATE TABLE #tbl
WITH
( DISTRIBUTION = ROUND_ROBIN
)
AS
SELECT  ROW_NUMBER() OVER(ORDER BY (SELECT NULL)) AS Sequence
,       schema_name(t.schema_id) as schema_name,
       t.name as table_name,
       t.create_date,
       t.modify_date
from sys.tables t
where t.name like 'databricks_streaming%'
--order by schema_name, table_name;
;
--select * from #tbl

DECLARE @nbr_statements INT = (SELECT COUNT(*) FROM #tbl)
,       @i INT = 1
;

WHILE   @i <= @nbr_statements
BEGIN
	DECLARE @tablename varchar(400)

	--Set @tablename = cast(@i AS VARCHAR(10))
	Set @tablename = (SELECT table_name FROM #tbl WHERE Sequence = @i)
	print @tablename

    DECLARE @sql_code NVARCHAR(4000) = 'IF OBJECT_ID(N'''+ @tablename +''', N''U'') IS NOT NULL Drop table ' + @tablename + ' ; ';
    EXEC    sp_executesql @sql_code;
    SET     @i +=1;
END

END
GO
```

Thank you!.