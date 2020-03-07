// Databricks notebook source
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

// COMMAND ----------

import com.microsoft.sqlserver.jdbc.SQLServerDataSource;

// COMMAND ----------

val dwDatabase = "idicdm"
val dwServer = "idicdmsvr.database.windows.net"
val dwUser = "adbingest"
val dwPass = "Adb!2345678"
val dwJdbcPort =  "1433"

// COMMAND ----------

val sqlDwUrlSmall = "jdbc:sqlserver://" + dwServer + ":" + dwJdbcPort + ";database=" + dwDatabase + ";user=" + dwUser+";password=" + dwPass

// COMMAND ----------

import java.util.Properties
val connectionProperties = new Properties()

connectionProperties.put("user", dwUser)
connectionProperties.put("password", dwPass)

// COMMAND ----------

val driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
connectionProperties.setProperty("Driver", driverClass)

// COMMAND ----------

    var connection:Connection = null
    var statement:Statement = null
    var sql = "select top(1) * from dbo.custdata10;"
    var updatesql = "update dbo.custdata10 Set state = 'IL1' where customername = 'idicustomer 6';"
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