# Testing Azure Synapse Analytics Update Statement

## Azure Synapse Demo tables creation

Create a table called custdata10.

```
DROP TABLE [dbo].[custdata10]
GO

/****** Object:  Table [dbo].[custdata10]    Script Date: 3/7/2020 1:22:11 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

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
```

Now insert sample data into custdata10

here are some samples

```
USE [databasename]
GO

INSERT INTO [dbo].[custdata10]
           ([eventdatetime]
           ,[customername]
           ,[address]
           ,[city]
           ,[state]
           ,[zip])
     VALUES
           ('2020-02-26T09:40:34.5648874-06:00'
           ,'idicustomer 6'
           ,'1435 Lake cook rd'
           ,'deerfield'
           ,'IL'
           ,'60015')
GO
```

## Download the JDBC Driver

Please go the below link and download the current driver.

https://docs.microsoft.com/en-us/sql/connect/jdbc/download-microsoft-jdbc-driver-for-sql-server?view=azure-sqldw-latest

Unzip the driver and you should see 3 drivers available choose JRE version 8 one for Azure databricks.

## Upload the JDBC Driver to Azure databricks cluster

Upload the JRE 8 version MS Sql jdbc driver into the cluster.

## Create code to do update statement

Create a new Notebook with Scala as language

Inlcude the necessary JDBC includes:

```
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
```

Here is how we can include Microsoft JDBC driver

```
import com.microsoft.sqlserver.jdbc.SQLServerDataSource;
```

Now configure the jdbc parameters

```
val dwDatabase = "databasename"
val dwServer = "servernameURL"
val dwUser = "username"
val dwPass = "Password"
val dwJdbcPort =  "1433"
```

Now build the JDBC URL

```
val sqlDwUrlSmall = "jdbc:sqlserver://" + dwServer + ":" + dwJdbcPort + ";database=" + dwDatabase + ";user=" + dwUser+";password=" + dwPass
```

Set Jdbc connection properties

```
import java.util.Properties
val connectionProperties = new Properties()

connectionProperties.put("user", dwUser)
connectionProperties.put("password", dwPass)
```

```
val driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
connectionProperties.setProperty("Driver", driverClass)
```

Now time to write the code to update a table in Azure Synapse Analytics

```
    var connection:Connection = null
    var statement:Statement = null
    var sql = "select top(1) * from dbo.custdata10;"
    //var updatesql = "update dbo.custdata10 Set state = 'IL1' where customername = 'idicustomer 6';"
     var updatesql = "update dbo.custdata10 set state = 'IL1' From dbo.custdata10 Join product on custdata10.customername = product.customername where custdata10.customername = 'idicustomer 7';"
    try {      
      connection = DriverManager.getConnection(sqlDwUrlSmall)      
      statement = connection.createStatement()
      statement.executeUpdate(updatesql)      
    } catch {      
                case e : Throwable => {e.printStackTrace
                throw e;
                }
    } finally {
        if (statement != null) {
          statement.close();
        }
        if (connection != null) {
          connection.close();
        }
    }
```

Now go back to Sql server management studio and run this query 

```
select * from dbo.custdata10 where customername = 'idicustomer 6';
```

the data displayed should show state as IL1 which mean it got updated.
