// Databricks notebook source
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

// COMMAND ----------

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


// COMMAND ----------

import com.microsoft.sqlserver.jdbc.SQLServerDataSource;

// COMMAND ----------

dbutils.widgets.text("tableid", "10")
val tableid = dbutils.widgets.get("tableid")

// COMMAND ----------

print(tableid)

// COMMAND ----------

        // Create datasource.
        SQLServerDataSource ds = new SQLServerDataSource();
        ds.setUser("<user>");
        ds.setPassword("<password>");
        ds.setServerName("<server>");
        ds.setPortNumber(Integer.parseInt("<port>"));
        ds.setDatabaseName("AdventureWorks");

        try (Connection con = ds.getConnection();
                CallableStatement cstmt = con.prepareCall("{call dbo.uspGetEmployeeManagers(?)}");) {
            // Execute a stored procedure that returns some data.
            cstmt.setInt(1, 50);
            ResultSet rs = cstmt.executeQuery();

            // Iterate through the data in the result set and display it.
            while (rs.next()) {
                System.out.println("EMPLOYEE: " + rs.getString("LastName") + ", " + rs.getString("FirstName"));
                System.out.println("MANAGER: " + rs.getString("ManagerLastName") + ", " + rs.getString("ManagerFirstName"));
                System.out.println();
            }
        }
        // Handle any errors that may have occurred.
        catch (SQLException e) {
            e.printStackTrace();
        }

// COMMAND ----------

val blobStorage = "waginput.blob.core.windows.net"
val blobContainer = "iditemp"
val blobAccessKey =  "TBwLoPOLim87APX5grtZzWy8Td9h69F/BJgDxuiQyEP480Cs5zyOa2bUeVVRfUnCALOug3aA2Wb4cj8aIiqGEw=="

// COMMAND ----------

val tempDir = "wasbs://" + blobContainer + "@" + blobStorage +"/stage"

// COMMAND ----------

val acntInfo = "fs.azure.account.key."+ blobStorage
sc.hadoopConfiguration.set(acntInfo, blobAccessKey)

// COMMAND ----------

//SQL Data Warehouse related settings
val dwDatabase = "idicdm"
val dwServer = "idicdmsvr.database.windows.net"
val dwUser = "adbingest"
val dwPass = "Adb!2345678"
val dwJdbcPort =  "1433"
val dwJdbcExtraOptions = "encrypt=true;trustServerCertificate=true;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
val sqlDwUrl = "jdbc:sqlserver://" + dwServer + ":" + dwJdbcPort + ";database=" + dwDatabase + ";user=" + dwUser+";password=" + dwPass + ";$dwJdbcExtraOptions"
val sqlDwUrlSmall = "jdbc:sqlserver://" + dwServer + ":" + dwJdbcPort + ";database=" + dwDatabase + ";user=" + dwUser+";password=" + dwPass

// COMMAND ----------

spark.conf.set(
    "spark.sql.parquet.writeLegacyFormat",
    "true")



// COMMAND ----------

val checkpointLocation = "wasbs://deltaidi@waginput.blob.core.windows.net/deltaidi/delta_custdata" + tableid + "/_checkpoints/etl-from-json"
//val deltapath = "wasbs://deltaidi@waginput.blob.core.windows.net/deltaidi/delta_custdata" + tableid
val deltapath = "wasbs://deltaidi@waginput.blob.core.windows.net/deltaidi/delta_custdata"
val checkpointLocationforsqldw = "wasbs://deltaidi@waginput.blob.core.windows.net/deltaidi/delta_custdata" + tableid + "/_checkpoints/etl-from-json_sqldw1"

// COMMAND ----------

dbutils.fs.rm("wasbs://deltaidi@waginput.blob.core.windows.net/deltaidi/delta_custdata"+ tableid +"/_checkpoints/etl-from-json_sqldw1", true)

// COMMAND ----------

val messages = spark.readStream.format("delta").option("ignoreDeletes", "true").option("ignoreChanges", "true").option("path", deltapath).table("delta_custdata").select("eventdatetime","customername","address","city","state","zip")

// COMMAND ----------

//messages.writeStream.select("eventdatetime","customername","address","city","state","zip")
val tablename = "custdata" + tableid
messages.writeStream
  .format("com.databricks.spark.sqldw")
  .option("url", sqlDwUrlSmall)
  .option("tempDir", tempDir)
  .option("forwardSparkAzureStorageCredentials", "true")
  .option("dbTable", tablename)
  .option("checkpointLocation", checkpointLocationforsqldw)
  .start()

// COMMAND ----------

//follow this tutorial
//https://github.com/MicrosoftDocs/azure-docs/blob/master/articles/azure-databricks/databricks-extract-load-sql-data-warehouse.md