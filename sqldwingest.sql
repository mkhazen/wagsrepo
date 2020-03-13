/****** Script for SelectTopNRows command from SSMS  ******/
SELECT *
  FROM [dbo].[custdata1]

  Use Master;

  CREATE LOGIN adbingest WITH PASSWORD = 'Adb!2345678';

  -- execute in the database
  CREATE USER adbingest FROM LOGIN adbingest; 

  ALTER ROLE dbmanager ADD MEMBER adbingest; 

  GRANT ALTER ANY USER TO adbingest;

  ALTER ROLE db_owner ADD MEMBER adbingest; 

  --worked
  EXEC sp_addrolemember 'db_owner', 'adbingest';

-- https://docs.microsoft.com/en-us/azure/sql-data-warehouse/resource-classes-for-workload-management

  SELECT name
FROM   sys.database_principals
WHERE  name LIKE '%rc%' AND type_desc = 'DATABASE_ROLE';

EXEC sp_addrolemember 'xlargerc', 'adbingest';

EXEC sp_addrolemember 'staticrc20', 'adbingest';
EXEC sp_addrolemember 'staticrc40', 'adbingest';
EXEC sp_addrolemember 'staticrc50', 'adbingest';

EXEC sp_droprolemember 'xlargerc', 'adbingest';
EXEC sp_droprolemember 'staticrc20', 'adbingest';

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

select count(*) from custdata10;
select count(*) from custdata11;
select count(*) from custdata12;
select count(*) from custdata13;
select count(*) from custdata14;
select count(*) from custdata15;
select count(*) from custdata16;
select count(*) from custdata17;
select count(*) from custdata18;
select count(*) from custdata19;

exec usp_createtable 20,100

exec usp_droptables 20,100


select count(*) from custdata20;

select count(*) from custdata119;

truncate table custdata20;
truncate table custdata21;
truncate table custdata22;
truncate table custdata23;
truncate table custdata24;
truncate table custdata25;
truncate table custdata26;
truncate table custdata27;
truncate table custdata28;
truncate table custdata29;

select count(*) from custdata20;
select count(*) from custdata21;
select count(*) from custdata22;
select count(*) from custdata23;
select count(*) from custdata24;
select count(*) from custdata25;
select count(*) from custdata26;
select count(*) from custdata27;
select count(*) from custdata28;
select count(*) from custdata29;

Drop table dbo.databricks_*;


select count(*) from custdata10;

select schema_name(t.schema_id) as schema_name,
       t.name as table_name,
       t.create_date,
       t.modify_date
from sys.tables t
order by schema_name,
         table_name;

select schema_name(t.schema_id) as schema_name,
       t.name as table_name,
       t.create_date,
       t.modify_date
from sys.tables t
where t.name like 'databricks_streaming%'
order by schema_name,
         table_name;

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
select * from #tbl

exec usp_cleanDatabrickStreamingtables;

select * From dbo.custdata10 where customername = 'idicustomer 7';

update dbo.custdata10 set state = 'IL1' where customername = 'idicustomer 7';

update dbo.custdata10 set state = 'IL' where customername = 'idicustomer 7';

update dbo.custdata10 set state = 'IL1' From dbo.custdata10 Join product on custdata10.customername = product.customername
where custdata10.customername = 'idicustomer 7';

/****** Object:  Table [dbo].[custdata1]    Script Date: 3/11/2020 1:58:54 PM ******/
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
	[satime] datetime NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	CLUSTERED COLUMNSTORE INDEX
)
GO


select * from dbo.custdatastream

truncate table dbo.custdatastream;



