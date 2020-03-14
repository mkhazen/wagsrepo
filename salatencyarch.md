# Loading Data into Azure synapse analytics using Stream Analytics

## End to End Architecture to Check the Latency

![alt text](https://github.com/balakreshnan/wagsrepo/blob/master/images/salatencyarch.jpg "Architecture")

## setup Azure synpase analytics

Create table called custdatastream in dbo schema

```
DROP TABLE [dbo].[custdatastream]
GO

/****** Object:  Table [dbo].[custdata1]    Script Date: 3/11/2020 1:58:54 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[custdatastream]
(
	[eventdatetime] [varchar](400) NULL,
	[customername] [varchar](300) NULL,
	[address] [varchar](500) NULL,
	[city] [varchar](100) NULL,
	[state] [varchar](50) NULL,
	[zip] [varchar](50) NULL,
	[inserttime] datetime NULL,
	[satime] datetime NULL,
	EventProcessedUtcTime datetime null,
    EventEnqueuedUtcTime datetime null,
    PartitionId int null
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	CLUSTERED COLUMNSTORE INDEX
)
GO
```

Query to retrieve data once data is loaded:

```
select eventdatetime, EventEnqueuedUtcTime as EventLanded, EventProcessedUtcTime as SAProcessed, satime from dbo.custdatastream;
```

## Goal

Goal here is to load event into Event hub and then use Azure Stream analytics to load those data into Azure synapse analytics in a streaming mode.

First create a Event hub namespace and event hub. Leave the default settings for parition. usually it is 4

Now Create a Azure stream analytics with default settings as well with 3 units and base version.

COnfigure the input as Event hub

Configure the output as Azyre Syanpse Analytics

Now time to write the query for Azure Stream Analytics

```
SELECT
    eventdatetime, customername, address, city, state, zip, 
    System.Timestamp() inserttime, System.Timestamp() satime,
    EventProcessedUtcTime, EventEnqueuedUtcTime, PartitionId
INTO
    output
FROM
    input
```

Now it is time to start the Azure stream analytics and wait until it start and status is showing running.

Now time to load some data. I am resuing the data generator from my other blogs. The code is available here.

https://github.com/balakreshnan/wagsrepo/blob/master/datagen.md

Use visual studio 2019 and open the project and run and wait untill 100 events are created. Then you can stop the code.

Now go back into SQL Management Studio or in the Azure portal open the Query editor and type the query as

```
select eventdatetime, EventEnqueuedUtcTime as EventLanded, EventProcessedUtcTime as SAProcessed, satime from dbo.custdatastream;
```

The results should like below:

![alt text](https://github.com/balakreshnan/wagsrepo/blob/master/images/saimg1.jpg "output")

Check the time differences between eventdatetime, EventLanded, SAProcessed

Eventdatetime - is when the Event was created by the data loader
EventLanded - is the time when Event reached Event hub.
SAProcessed - is the time when SA processed the record and saved

Almost less that a second Stream Analytics is reading and processing.