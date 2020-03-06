// Databricks notebook source
// MAGIC %md
// MAGIC ### CDC tool for Databricks Delt to Azure Synapse Analytics
// MAGIC ##### Worker notebook to work on Change Data Capture for configured tables in a Bundle Batch

// COMMAND ----------

import org.apache.spark.sql._
import org.apache.log4j.{LogManager, Level}
import org.apache.commons.logging.LogFactory
import org.apache.log4j.PatternLayout
import spark.implicits._
import org.apache.spark.sql._
import scala.collection.mutable.MutableList
import java.util.ArrayList
import com.edmunds.rest.databricks.restclient.DatabricksRestClient;
import com.edmunds.rest.databricks._;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import com.edmunds.rest.databricks.DTO.jobs._;
import com.edmunds.rest.databricks.DTO._;
import com.edmunds.rest.databricks.restclient.DatabricksRestClientImpl;
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.functions._
import java.sql.DriverManager
import java.sql.Connection
import java.sql.Statement
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.streaming.StreamingQueryListener.QueryStartedEvent
import org.apache.spark.sql.streaming.StreamingQueryListener.QueryTerminatedEvent
import org.apache.spark.sql.streaming.StreamingQueryListener.QueryProgressEvent
import org.apache.spark.streaming.scheduler.StreamingListener
import org.apache.spark.streaming.scheduler.StreamingListenerBatchCompleted
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.streaming.StreamingQueryListener._
import org.apache.http.client.{ResponseHandler, HttpClient}
import org.apache.http.client.methods.HttpPost
import org.apache.http.impl.client.{BasicResponseHandler, DefaultHttpClient, HttpClientBuilder}
import org.apache.http.entity.StringEntity
import org.json4s.jackson.JsonMethods._
import java.time.LocalDateTime
import java.sql.ResultSet;

LogManager.getRootLogger().setLevel(Level.DEBUG)
val log = LogFactory.getLog("Databricks_Delta_To_Synapse_Analytics_log:")

// COMMAND ----------

//dbutils.widgets.text("BundleBatchId","batch-1")
//dbutils.widgets.text("jobIdCreated","none")
//dbutils.widgets.removeAll()
dbutils.widgets.text("notebook", dbutils.notebook.getContext().notebookPath.get)
val bundleBatchIdToProcess = dbutils.widgets.get("BundleBatchId")
var jobIdCreated = dbutils.widgets.get("jobIdCreated");

// COMMAND ----------

// MAGIC %md
// MAGIC Configurations that will be moved to secrets scope

// COMMAND ----------

//TODO: use secrets scope for credentials
val storageAccount = "walgreens-demo"
spark.conf.set("fs.azure.account.key.walgreensdemo.dfs.core.windows.net","==")
val jdbcURI ="jdbc:sqlserver://josh-seidel-dw-s-demo.database.windows.net:1433;database=superpool;user=dwlab@josh-seidel-dw-s-demo;password=@!;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"

val cacheDir = "abfss://walgreens-demo@walgreensdemo.dfs.core.windows.net/cacheDir/"
val checkpointsDir = "abfss://walgreens-demo@walgreensdemo.dfs.core.windows.net/cacheDir/checkpoints/"

// COMMAND ----------

val heartbeatTableSchema = List(
      StructField("job_id", StringType, true),
      StructField("run_id", StringType, true),
      StructField("source_table_name", StringType, true),
      StructField("bundle_batch_id", StringType, true),
      StructField("records_processed_in_batch", LongType, true),
      StructField("batch_interval", LongType, true),
      StructField("number_of_nodes", LongType, true) 
    )

//TODO: Change this method to use JDBC
def logHeartBeat(job_id:String, run_id:String, source_table_name:String, bundle_batch_id:String, records_processed_in_batch:Long, batch_inetrval:Long, number_of_nodes:Long) = {
  
    jobIdCreated = dbutils.widgets.get("jobIdCreated");
  
    val dataForLog = Seq(
      Row(jobIdCreated.toString, run_id.toString,source_table_name,bundle_batch_id,records_processed_in_batch,batch_inetrval,number_of_nodes)  
    ) 

    val logDataDF = spark.createDataFrame(
      spark.sparkContext.parallelize(dataForLog),
      StructType(heartbeatTableSchema)
    )
    val tsAddedDF = logDataDF.withColumn("created_at_datetime", current_timestamp())  
    val cacheDirLocaton = cacheDir+"logs/synch_delta_sqldw_heartbeat";
    tsAddedDF.write
              .format("com.databricks.spark.sqldw")     
              .option("url", jdbcURI)
              .mode("append")  
              .option("forward_spark_azure_storage_credentials", "true")
              .option("dbtable", "synch_delta_sqldw_heartbeat")
              .option("tempdir", cacheDirLocaton)
              .save()
}

// COMMAND ----------

def getUpsertQueryForSQLDW (tmpTabelNameInSQLDW: String, masterTableNameInSQLDW: String, primaryKey: String) : String = {
  
      val ctasUpsertQuery = """CREATE TABLE dbo."""+masterTableNameInSQLDW+"""_upsert
      WITH
      (   DISTRIBUTION = HASH(["""+primaryKey+"""])
        ,   CLUSTERED INDEX (["""+primaryKey+"""])
      )
      AS 
      SELECT * 
      FROM
      dbo."""+tmpTabelNameInSQLDW+""" AS s
      UNION ALL  
      SELECT  *
      FROM
      dbo."""+masterTableNameInSQLDW+""" AS p
      WHERE NOT EXISTS
      (   SELECT  *
          FROM    [dbo].["""+tmpTabelNameInSQLDW+"""] s
          WHERE   s.["""+primaryKey+"""] = p.["""+primaryKey+"""]
      );"""
  
return ctasUpsertQuery;
}

// COMMAND ----------


case class SynchDeltaSqldwTask (source_table_name:String, target_table_name:String, skip_columns:String, key_columns:String, active_flag:Integer, bundle_batch_id:String, upsert_in_target_flag:Int )

def checkIfTableExistsOnSQLDW(table: String) : Boolean = {
    var connection:Connection = null
    var statement:Statement = null
    try {      
      connection = DriverManager.getConnection(jdbcURI)      
      statement = connection.createStatement()
      statement.executeQuery("select top (1) * from "+table)  
      //NOTEL We are looking for this EXCEPTION if table does not exist in Synapse Analytics
      //com.microsoft.sqlserver.jdbc.SQLServerException: Invalid object name 'dbo.test2'
    } catch {
                case e: Throwable => {
                  e.printStackTrace
                  if(e.getMessage.contains("Invalid object name")){
                    return false;
                  } else{
                   // We are letting the job fail if it is some other type of exception 
                  throw e;
                  }
                }
    } finally {
        if (statement != null) {
          statement.close();
        }
        if (connection != null) {
          connection.close();
        }
    }
    return true;
  
}

def runSQLStatementInSQLDW(sql:String) : Boolean = {
  
    var connection:Connection = null
    var statement:Statement = null
    try {      
      connection = DriverManager.getConnection(jdbcURI)      
      statement = connection.createStatement()
      statement.executeUpdate(sql)      
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
    return true;
}

//TODO: change this method to JDBC
def getActiveSynchDeltaSqldwTasksForBundleBatchId(bundleBatchId:String) : java.util.List[SynchDeltaSqldwTask] = {
   val dfOfSynchDeltaSqldwTasksForBundleBatchId: DataFrame = spark.read
  .format("com.databricks.spark.sqldw")
  .option("url", jdbcURI)
  .option("tempDir", cacheDir)
  .option("forwardSparkAzureStorageCredentials", "true")
  .option("query", "select * from dbo.synch_delta_sqldw_tasks where active_flag=1 and bundle_batch_id='"+bundleBatchId+"'")
  .load()
  val synchDeltaSqldwTasksList = dfOfSynchDeltaSqldwTasksForBundleBatchId.as[SynchDeltaSqldwTask].collectAsList
  return synchDeltaSqldwTasksList
}

// COMMAND ----------

// MAGIC %md
// MAGIC #####This method is invoked foreachbatch

// COMMAND ----------

//This method is invoked foreachbatch
def processDataToSynapseAnalytics(microBatchDF: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row], batchId: scala.Long) 
{
   println("processDataToSynapseAnalytics invoked")
   microBatchDF.cache();
   var tlist = microBatchDF.select("targetTableNameToWrite","bundleBatchId","targetTableNamePrimaryKeys","upsert_in_target_flag")
   var tlist2 = tlist.dropDuplicates("targetTableNameToWrite")
   var targetTableName = ""; 
   var bundleBatchId = "";
   var targetTableKeys = "";
   var upsertInTargetFlag = "";
   tlist2.rdd.collect().map(row => {
          println("table name: "+row.getAs[String](0))
          targetTableName = row.getAs[String](0);
          bundleBatchId = row.getAs[String](1);
          targetTableKeys = row.getAs[String](2);
          upsertInTargetFlag = row.getAs[String](3);
    })
   //dropping columns used for management before write (TODO: can be improved with params to foreachbatch method)
   microBatchDF.drop("targetTableNameToWrite","bundleBatchId","targetTableNamePrimaryKeys","upsert_in_target_flag")
  
   val uuid =  java.util.UUID.randomUUID().toString    
   val eventsToProcessMicrobatchUniqueKeyAdded = microBatchDF.withColumn("MicroBatchDBUniqueKey", lit(uuid))
   val eventsToProcessWithTS = eventsToProcessMicrobatchUniqueKeyAdded.withColumn("MicroBatchBeingWrittenFromDBToSQLDWAtDateTime", current_timestamp())  

  
   val cacheDirLocaton = cacheDir+targetTableName;
   val tmpCacheDirLocaton = cacheDir+"tmp/"+targetTableName;
  
   if(upsertInTargetFlag.equals("1")){
           //This block is only used where upsert is required in target table in Synapse Analytics
           if (checkIfTableExistsOnSQLDW(targetTableName) == false) {
               //This is done if we are writing for the first time and table does not exist on Synapse Analytics
               eventsToProcessWithTS.write
                                  .format("com.databricks.spark.sqldw")     
                                  .option("url", jdbcURI)
                                  .mode("append")  
                                  .option("forward_spark_azure_storage_credentials", "true")
                                  .option("dbtable", targetTableName)
                                  .option("tempdir", tmpCacheDirLocaton)
                                  .save()
               runSQLStatementInSQLDW("truncate table "+targetTableName);
           }
           try {
                runSQLStatementInSQLDW("drop table "+targetTableName+"_upsert");     
           }  catch {
                    case e: Throwable => {}            
           }
           eventsToProcessWithTS.write
                                .format("com.databricks.spark.sqldw")                        
                                .option("url", jdbcURI)
                                .mode("append")  
                                .option("forward_spark_azure_storage_credentials", "true")
                                .option("dbtable", targetTableName+"_stg")
                                .option("tempdir", cacheDirLocaton)
                                .save()  
          
           //TODO: log how long upsert takes on Synapse Analytic
           val upsertQuery = getUpsertQueryForSQLDW(targetTableName+"_stg", targetTableName, targetTableKeys);
           runSQLStatementInSQLDW(upsertQuery);
     
           try {
               runSQLStatementInSQLDW("drop table "+targetTableName+"_old");   
               runSQLStatementInSQLDW("drop table "+targetTableName+"_stg");
           }  catch {
                    case e: Throwable => {}              
           }
           runSQLStatementInSQLDW("rename object dbo."+targetTableName+" to "+targetTableName+"_old");
           runSQLStatementInSQLDW("rename object dbo."+targetTableName+"_upsert to "+targetTableName); 

   }  else {
        //This bloick is only used for tables which require append only on Synapse Analytics
        //TODO: look at atleast once semantics and use the above block even for append only tables.
        eventsToProcessWithTS.write
                                  .format("com.databricks.spark.sqldw")                        
                                  .option("url", jdbcURI)
                                  .mode("append")  
                                  .option("forward_spark_azure_storage_credentials", "true")
                                  .option("dbtable", targetTableName)
                                  .option("tempdir", cacheDirLocaton)
                                  .save()      
   }
  
   val count = microBatchDF.count()
   microBatchDF.unpersist();
   println("Exiting function processDataToSynapseAnalytics")  
   
   val num_workers_on_cluster =  sc.statusTracker.getExecutorInfos.length - 1;
   logHeartBeat(jobIdCreated,"NA",targetTableName,bundleBatchId,count,0,num_workers_on_cluster);  

} // end of function

// COMMAND ----------

// MAGIC %md
// MAGIC ~~~
// MAGIC /*********************
// MAGIC Main Logic:
// MAGIC Load all tasks for the input Bundle Batch Id from database config table
// MAGIC For each task start the CDC and write to Synapse Analytics
// MAGIC **********************/
// MAGIC ~~~

// COMMAND ----------


  val synchDeltaSqldwTasksList = getActiveSynchDeltaSqldwTasksForBundleBatchId(bundleBatchIdToProcess);
  val numberOfTablesForCDCInJob = synchDeltaSqldwTasksList.size;
  dbutils.widgets.text("numberOfTablesForCDCInJob", numberOfTablesForCDCInJob.toString)
  val synchDeltaSqldwTasksListIterator = synchDeltaSqldwTasksList.iterator

  while(synchDeltaSqldwTasksListIterator.hasNext()) {
    
      val synchDeltaSqldwTask = synchDeltaSqldwTasksListIterator.next();
      //TODO: change events to actual source later
      
      val eventsToProcess = spark.readStream.format("delta").option("ignoreChanges", "true").table(synchDeltaSqldwTask.source_table_name)
      val eventsToProcessWithTargetTableName = eventsToProcess.withColumn("targetTableNameToWrite", lit(synchDeltaSqldwTask.target_table_name))
      val eventsToProcessWithPrimaryKeys = eventsToProcessWithTargetTableName.withColumn("targetTableNamePrimaryKeys", lit(synchDeltaSqldwTask.key_columns))    
      val eventsToProcessWithBatchBundleId = eventsToProcessWithPrimaryKeys.withColumn("bundleBatchId", lit(bundleBatchIdToProcess))
      val skipColumnList: List[String] = synchDeltaSqldwTask.skip_columns.split(",").map(_.trim).toList
      val afterSkippingColumnsDF = eventsToProcessWithBatchBundleId.drop(skipColumnList : _*)
      val data = afterSkippingColumnsDF.withColumn("upsert_in_target_flag", lit(synchDeltaSqldwTask.upsert_in_target_flag.toString))
      println("Starting synch for target table "+synchDeltaSqldwTask.target_table_name)
      val cacheDirLocaton = cacheDir+synchDeltaSqldwTask.target_table_name;
      val checkpointLocation = checkpointsDir+"checkpointsDir/"+synchDeltaSqldwTask.target_table_name;
      println("creating stream and Running for "+synchDeltaSqldwTask);      
      //TODO: How to pass custom parameters to foreachBatch method other than the default parameters?
    
      data
      .writeStream
      .option("checkpointLocation", checkpointLocation)
      .queryName("Target-Table:"+synchDeltaSqldwTask.target_table_name+"Job-ID:"+jobIdCreated)
      .foreachBatch(processDataToSynapseAnalytics _)
      .start();
     
  }

// COMMAND ----------

val streamListener = new StreamingQueryListener() {
  
    var lowCount = 0
    var highCount = 0
    val clusterId = dbutils.notebook.getContext.clusterId.get  
    val bundleBatchId = dbutils.widgets.get("BundleBatchId")   
    val jdbcURI ="jdbc:sqlserver://josh-seidel-dw-s-demo.database.windows.net:1433;database=superpool;user=dwlab@josh-seidel-dw-s-demo;password=juk9999@!;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
    val url = "https://eastus2.azuredatabricks.net/api/2.0/clusters/resize"
  
    val slaQuery = """
     select sla_minutes from dbo.synch_delta_sqldw_bundle_batch_sla
     where active_flag=1 and bundle_batch_id='"""+bundleBatchId+"""'
    """
  
    val token = dbutils.notebook.getContext().apiToken.get
    val numberOfTablesForCDCInJob = dbutils.widgets.get("numberOfTablesForCDCInJob");
    
  
    val maxBatchIntervalSQL = """
    select top(1) avg_batch_interval,job_id from 
    (
    select avg(batch_interval) as avg_batch_interval , job_id
    from 
    dbo.synch_delta_streaming_query_log
    where 
    cluster_id = '"""+clusterId+"""'
    and created_at_datetime <= dateadd(minute, -30, getdate())
    group by job_id
    ) c
    order by avg_batch_interval desc
    """; 
  
    case class MaxBatchInterval (
      avgBatchInterval: Long,
      batchId: String    
    )
  
    case class Sla (
      slaMinutes: Int    
    )
  
    def getSlaForBundleBatchFromConfigTable(sql:String) : Sla = {  
    
        var connection:Connection = null
        var statement:Statement = null
        var resultSet:ResultSet = null
        var resultObject: Sla = null 
      
        try {      
          connection = DriverManager.getConnection(jdbcURI)      
          statement = connection.createStatement()
          resultSet = statement.executeQuery(sql)      
          while ( resultSet.next() ) {        
            val slaMinutes = resultSet.getInt("sla_minutes")              
            resultObject = new Sla(slaMinutes);
          }       
        } catch {
                    case e: Throwable => {e.printStackTrace
                    throw e;
                    }
        } finally {
            if (statement != null) {
              statement.close();
            }
            if (resultSet != null) {
              resultSet.close();
            }
            if (connection != null) {
              connection.close();
            }
        }

        return resultObject;
 } 
  
  
   def getMaxBatchIntervalAcrossAllQueries(sql:String) : MaxBatchInterval = {  
    
        var connection:Connection = null
        var statement:Statement = null
        var resultSet:ResultSet = null
        var resultObject: MaxBatchInterval = null 
     
        try {      
          connection = DriverManager.getConnection(jdbcURI)      
          statement = connection.createStatement()
          resultSet = statement.executeQuery(sql)

          while ( resultSet.next() ) {        
            val avg_batch_interval = resultSet.getLong("avg_batch_interval")
            val rs_batch_id = resultSet.getString("job_id")        
            resultObject = new MaxBatchInterval(avg_batch_interval,rs_batch_id);
          }       
        } catch {
                    case e: Throwable => {e.printStackTrace
                    throw e;
                    }
        } finally {
            if (statement != null) {
              statement.close();
            }
            if (resultSet != null) {
              resultSet.close();
            }
            if (connection != null) {
              connection.close();
            }
        }
        return resultObject;
  }   
  
  
  def getRestContent(url:String, jsonString:String): String = {

      val httpClient = HttpClientBuilder.create().build()
      val post = new HttpPost(url)
    
      post.setEntity(new StringEntity(jsonString))
      post.addHeader("Authorization", s"Bearer $token")

      val httpResponse = httpClient.execute(post)
      val entity = httpResponse.getEntity()
      var content = ""
      if (entity != null) {
        val inputStream = entity.getContent()
        content = scala.io.Source.fromInputStream(inputStream).getLines.mkString
        inputStream.close
      }
      return content
    }
  
  def runSQLStatementInSQLDW_1(sql:String) : Boolean = {
  
    var connection:Connection = null
    var statement:Statement = null
    try {      
      connection = DriverManager.getConnection(jdbcURI)      
      statement = connection.createStatement()
      statement.executeUpdate(sql)      
    } catch {
                case e: Throwable => {
                  e.printStackTrace
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
    
    return true;
}

  def logActivityFromStreamingListener( job_id:String,
                                       cluster_id:String,
                                       number_of_nodes:Long,
                                       target_table_name:String,
                                       records_processed_in_batch:Long,
                                       bundle_batch_id:String,
                                       streaming_query_name:String,
                                       streaming_query_id:String,
                                       batch_interval:Long) : Unit = {
       
    val currentTimeStamp = new java.sql.Timestamp(System.currentTimeMillis())
    val sql = """insert into synch_delta_streaming_query_log
    ( 
    job_id,
    cluster_id,
    number_of_nodes,
    target_table_name,
    records_processed_in_batch,
    bundle_batch_id,
    streaming_query_name,
    streaming_query_id,
    batch_interval,
    created_at_datetime
    ) 
    values (
    '"""+job_id+"""',
    '"""+cluster_id+"""',
    """+number_of_nodes+""",
    '"""+target_table_name+"""',
    """+records_processed_in_batch+""",
    '"""+bundle_batch_id+"""',
    '"""+streaming_query_name+"""',
    '"""+streaming_query_id+"""',
    """+batch_interval+""",
    '"""+currentTimeStamp+"""');"""
       
    runSQLStatementInSQLDW_1(sql);
  }
  
 

  override def onQueryProgress(queryProgress: QueryProgressEvent): Unit = {  
  
  logActivityFromStreamingListener( queryProgress.progress.name,
                                   clusterId,
                                   sc.statusTracker.getExecutorInfos.length - 1,
                                   queryProgress.progress.name,
                                   queryProgress.progress.numInputRows,
                                   bundleBatchId,
                                   queryProgress.progress.name,
                                   queryProgress.progress.batchId.toString,
                                   queryProgress.progress.durationMs.get("triggerExecution").toLong)
    
    val scaleInterval = 2
    println(s"Low count is $lowCount")
    println(s"High count is $highCount")    
    val num_workers =  sc.statusTracker.getExecutorInfos.length - 1
    
    val numWorkersUp =  if (num_workers > 3 ) {
        (num_workers * 1.25).toInt 
    } else {
      num_workers + 1
    }
    
    val numWorkersDown = (num_workers * 0.75).toInt + 1 // make sure there is at least one worker
    
    val jsonStringUp =
    s"""{
      |"cluster_id": "$clusterId",
      |"num_workers": $numWorkersUp}""".stripMargin

    val jsonStringDown =
    s"""{
      |"cluster_id": "$clusterId",
      |"num_workers": $numWorkersDown}""".stripMargin

    //println("Query made progress: " + queryProgress.progress)
    val res = queryProgress.progress.toString()
    var batchInterval:Long = 10000L;    
    var processContinue = true;
    
    if (Integer.parseInt(numberOfTablesForCDCInJob)>1) {
      
         val maxBI =getMaxBatchIntervalAcrossAllQueries(maxBatchIntervalSQL);
         println("queryProgress.progress.name value is "+queryProgress.progress.name)
         println("maxBI is "+maxBI);
         if (maxBI != null && maxBI.batchId.equals(queryProgress.progress.name)) {
             println("MaxBI and query matched: batchinterval is "+maxBI.avgBatchInterval)
             batchInterval= maxBI.avgBatchInterval;
             processContinue = true
         } else {
             if(maxBI==null){
               processContinue = true;
             } else {
             println("Executing else block of batchId equals queryProgress.progress.name")
             processContinue = false;
             }  
         }
      
    } else {
        batchInterval = queryProgress.progress.durationMs.get("triggerExecution").toLong
        println("Normal batchinterval is "+batchInterval.toString)
    }
    
      println("processContinue value is "+processContinue);
    
      if (processContinue) {
         
              println("inside process continue block ");
              val sla = getSlaForBundleBatchFromConfigTable(slaQuery)
              var applied_sla = 300000 
              if(sla!=null && sla.slaMinutes > 0){
                applied_sla = sla.slaMinutes * 60000
              }
              println("applied_sla_by_user_in_config "+applied_sla.toString);              
             
              if (batchInterval > applied_sla) {
                highCount += 1
              }
              
              if (batchInterval < applied_sla) {
                lowCount += 1
              }
              println("num_workers:", num_workers, "numWorkersUp:", numWorkersUp, "numWorkersDown:", numWorkersDown, "highCount:", highCount , "scaleInterval:", scaleInterval)
              if (highCount >= scaleInterval){
                println(s"Scaling up to $numWorkersUp workers")
                getRestContent(url,jsonStringUp)
                highCount = 0
              }

              if (lowCount >= scaleInterval){
                println(s"Scaling down to $numWorkersDown workers")
                getRestContent(url,jsonStringDown)
                lowCount = 0
              }
       }
    
        
    
  }

  override def onQueryStarted(queryStarted: QueryStartedEvent): Unit = { }
  override def onQueryTerminated(queryTerminated: QueryTerminatedEvent): Unit = { }
}

// add the new listener callback
spark.streams.addListener(streamListener)