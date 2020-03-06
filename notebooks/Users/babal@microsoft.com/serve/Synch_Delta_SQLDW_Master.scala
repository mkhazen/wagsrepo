// Databricks notebook source
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
import com.edmunds.rest.databricks.DTO.jobs.JobSettingsDTO;
import com.edmunds.rest.databricks.DTO.libraries.LibraryDTO;
import com.edmunds.rest.databricks.DTO.libraries.MavenLibraryDTO;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.functions._

// COMMAND ----------

// MAGIC %md
// MAGIC Configurations will be loaded from secrets scope

// COMMAND ----------

val storageAccount = "walgreens-demo"
spark.conf.set("fs.azure.account.key.walgreensdemo.dfs.core.windows.net",dbutils.secrets.get(scope = storageAccount, key = "storage"))
val jdbcURI ="jdbc:sqlserver://josh-seidel-dw-s-demo.database.windows.net:1433;database=superpool;user=dwlab@josh-seidel-dw-s-demo;password=@!;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
val cacheDir = "abfss://walgreens-demo@walgreensdemo.dfs.core.windows.net/cacheDir"
val databricksTokenForAPICalls = "dapi6a13d6026ef9f43640e9149c91c5f07c"
val databricksDomainURLForAPICalls = "eastus2.azuredatabricks.net"

// COMMAND ----------

val jobsCreatedLogSchema = List(
      StructField("job_id", StringType, true),
      StructField("run_id", StringType, true),
      StructField("cluster_id", StringType, true),
      StructField("bundle_batch_id", StringType, true)  
    )

val df: DataFrame = spark.read
  .format("com.databricks.spark.sqldw")
  .option("url", jdbcURI)
  .option("tempDir", cacheDir)
  .option("forwardSparkAzureStorageCredentials", "true")
  .option("query", "select distinct bundle_batch_id from dbo.synch_delta_sqldw_tasks where active_flag=1")
  .load()

val bundleBatchIdList = new ArrayList[String]
df.collect().foreach(row => row.toSeq.foreach(col => {
      bundleBatchIdList.add(col.asInstanceOf[String])  
}))

// COMMAND ----------

// MAGIC %md
// MAGIC Method to check of a job is already in progress

// COMMAND ----------

def checkIfJobIsRunning( jobId:Long ) : Boolean = 
{
    var returnValue:Boolean = false  
    val builder1 = DatabricksServiceFactory.Builder.createTokenAuthentication(databricksTokenForAPICalls, databricksDomainURLForAPICalls);
    val sf1 = builder1.withMaxRetries(5).withRetryInterval(10000L).build(); 
    var runsDTO : com.edmunds.rest.databricks.DTO.RunsDTO = null;
    try {
       runsDTO = sf1.getJobService().listRuns(jobId, true, 20, 10)
    } catch {
    case e: DatabricksRestException => {
      println("DatabricksRestException occurred when callin listRuns method on Jobservice")
      e.printStackTrace
      return false;
     }
    case e: IOException => {
      println("Had an IOException trying to reach REST API ")
      e.printStackTrace
      throw e;
       }
    }
    if(runsDTO != null)
    {
        val runs = runsDTO.getRuns();
        if(runs!=null && runs.length > 0) 
        {
             println(runs(0).getState().getLifeCycleState())
             returnValue = true;
             // val status = runs(0).getState().getLifeCycleState();
             // RUNNING is the value that needs to be state for active runid
        }  
    }  
  
    return returnValue
} // end of function

// COMMAND ----------

// MAGIC %md
// MAGIC Logic to only pick bundle batch ids that are not active. 
// MAGIC Start CDC on all active tables in that bundle

// COMMAND ----------

df.collect().foreach(row => row.toSeq.foreach(col => {
     val bundleBatchIdToProcess = col.asInstanceOf[String]
     println("processing bundle: "+bundleBatchIdToProcess)
  
     val dfBatchIdJobDetails: DataFrame = spark.read
                                          .format("com.databricks.spark.sqldw")
                                          .option("url", jdbcURI)
                                          .option("tempDir", cacheDir)
                                          .option("forwardSparkAzureStorageCredentials", "true")
                                          .option("query", "select s1.job_id from dbo.synch_delta_sqldw_heartbeat s1 inner join ( select bundle_batch_id, max(created_at_datetime) as mts from dbo.synch_delta_sqldw_heartbeat group by bundle_batch_id ) s2 on s2.bundle_batch_id = s1.bundle_batch_id and s1.created_at_datetime = s2.mts and s1.bundle_batch_id='"+bundleBatchIdToProcess+"'")
  .load()
    display(dfBatchIdJobDetails);
     dfBatchIdJobDetails.collect().foreach(row => row.toSeq.foreach(col => 
     {
          var jobIdToCheck = null;
          var jobIsRunning = false;
          try { 
             println(col.asInstanceOf[String]);
             jobIsRunning = checkIfJobIsRunning((col.asInstanceOf[String]).toLong);  
          } catch {
                   case e => {
                     e.printStackTrace    
                     throw e
                   }
          }
         
          if(jobIsRunning){
                println("true: Job is running")
                //Remove from list as the job is already in progress
                bundleBatchIdList.remove(bundleBatchIdToProcess)
           } 
     }))

}))

// COMMAND ----------

// MAGIC %md
// MAGIC Method to create a Databricks Job using REST API

// COMMAND ----------

import scala.collection.JavaConverters._


def createJob(bundleBatchId: String) : Long = {
  
  val builder1 = DatabricksServiceFactory.Builder.createTokenAuthentication(databricksTokenForAPICalls, databricksDomainURLForAPICalls);
  val sf1 = builder1.withMaxRetries(5).withRetryInterval(10000L).build();   
  val jobSettingsDTO = new JobSettingsDTO();  
  
  val newClusterDTO = new NewClusterDTO();
  newClusterDTO.setNumWorkers(2);
  //newClusterDTO.setNodeTypeId("Standard_F4s"); 
 // newClusterDTO.setNodeTypeId("Standard_F8s");
  //newClusterDTO.setDriverNodeTypeId("Standard_F4s");  
  newClusterDTO.setSparkVersion("6.2.x-scala2.11")
  newClusterDTO.setInstancePoolId("0306-141747-edgy820-pool-sTxg1JkG")
  
  val notebookTaskDTO = new NotebookTaskDTO();
  notebookTaskDTO.setNotebookPath("/Users/srinivas.nelakuditi@databricks.com/walgreens/delta_sqldw_synch/Synch_Delta_SQLDW_Job_Run");
    
  var baseParameters: java.util.Map[String, String] = new java.util.HashMap[String, String]
  baseParameters.put("BundleBatchId",bundleBatchId)
  
  val libraryDTO = new LibraryDTO();
  val mavenLibraryDTO = new MavenLibraryDTO();
  mavenLibraryDTO.setCoordinates("com.edmunds:databricks-rest-client:2.6.1");
  libraryDTO.setMaven(mavenLibraryDTO)
  notebookTaskDTO.setBaseParameters(baseParameters);
   
  var libArray = new Array[com.edmunds.rest.databricks.DTO.libraries.LibraryDTO](1) 
 
  libArray(0) = libraryDTO
  jobSettingsDTO.setNewCluster(newClusterDTO);
  jobSettingsDTO.setLibraries(libArray);
  jobSettingsDTO.setNotebookTask(notebookTaskDTO);
    
  val jobIdCreated = sf1.getJobService().createJob(jobSettingsDTO);
  baseParameters.put("jobIdCreated",jobIdCreated.toString)
  notebookTaskDTO.setBaseParameters(baseParameters);
  //we are calling reset as the job needs jobId to log to heart beat table.
  sf1.getJobService().reset(jobIdCreated, jobSettingsDTO)
  return jobIdCreated
  
}

// COMMAND ----------

// MAGIC %md
// MAGIC Method to run a job

// COMMAND ----------

def runJob(jobId: Long) : Long = {
  val builder1 = DatabricksServiceFactory.Builder.createTokenAuthentication(databricksTokenForAPICalls, databricksDomainURLForAPICalls);
  val sf1 = builder1.withMaxRetries(5).withRetryInterval(10000L).build();   
  val runNowDTO = sf1.getJobService().runJobNow(jobId);
  println("RUN ID created is "+runNowDTO.getRunId())
  return runNowDTO.getRunId();
}

def createJobAndRun(bundleBatchId:String) = {
    println("Creating new Job and Run for bundleBatchId: "+bundleBatchId)    
    val createdJobId = createJob(bundleBatchId)
    println("New Job created with Id: "+createdJobId);
    val runId = runJob(createdJobId);
    println("Run id created from runJob is: "+runId)  
  
    //TODO: Move this logging to a utility notebook and use JDBC
    val dataForLog = Seq(Row(createdJobId.toString, runId.toString,"none",bundleBatchId))
    val logDataDF = spark.createDataFrame(
      spark.sparkContext.parallelize(dataForLog),
      StructType(jobsCreatedLogSchema)
    )
    val tsAddedDF = logDataDF.withColumn("created_at_datetime", current_timestamp())  
    val cacheDirLocaton = "abfss://walgreens-demo@walgreensdemo.dfs.core.windows.net/cacheDir/logs/synch_delta_sqldw_jobs_created";
    tsAddedDF.write
    .format("com.databricks.spark.sqldw")     
    .option("url", jdbcURI)
    .mode("append")  
    .option("forward_spark_azure_storage_credentials", "true")
    .option("dbtable", "synch_delta_sqldw_jobs_created")
    .option("tempdir", cacheDirLocaton)
    .save()      
}  

// COMMAND ----------

val bundleBatchIdIterator = bundleBatchIdList.iterator
while(bundleBatchIdIterator.hasNext() ) {
      val bundleBatchIdToCreateJob = bundleBatchIdIterator.next()      
      val synchDeltaSqldwTasksList = createJobAndRun(bundleBatchIdToCreateJob);
 }