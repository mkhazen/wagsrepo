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




