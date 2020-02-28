// Databricks notebook source
    import scala.collection.JavaConverters._
    import com.microsoft.azure.eventhubs._
    import java.util.concurrent._
    import scala.collection.immutable._
    import scala.concurrent.Future
    import scala.concurrent.ExecutionContext.Implicits.global
import org.apache.spark.sql._ 
import org.apache.spark.sql.functions._

// COMMAND ----------

spark.conf.set(   "fs.azure.account.key.waginput.blob.core.windows.net", "TBwLoPOLim87APX5grtZzWy8Td9h69F/BJgDxuiQyEP480Cs5zyOa2bUeVVRfUnCALOug3aA2Wb4cj8aIiqGEw==")

// COMMAND ----------

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions._

// COMMAND ----------

val checkpointLocationproduct = "wasbs://deltaidi@waginput.blob.core.windows.net/deltaidi/delta_custdata/_checkpoints/etl-from-jsonproduct"
val deltapathproduct = "wasbs://deltaidi@waginput.blob.core.windows.net/deltaidi/delta_productdim"

// COMMAND ----------

val dfproduct = spark.read.option("header","true").option("inferSchema","true").csv("wasbs://ididims@waginput.blob.core.windows.net/productdim.csv")

// COMMAND ----------

val dfproduct1 = dfproduct.withColumn("Date", (col("eventdatetime").cast("date")))

// COMMAND ----------

dfproduct1.write.format("delta").partitionBy("Date").save(deltapathproduct)

// COMMAND ----------

val checkpointLocationsupplier = "wasbs://deltaidi@waginput.blob.core.windows.net/deltaidi/delta_custdata/_checkpoints/etl-from-jsonsupplier"
val deltapathsupplier = "wasbs://deltaidi@waginput.blob.core.windows.net/deltaidi/delta_supplierdim"

// COMMAND ----------

val dfsupplier = spark.read.option("header","true").option("inferSchema","true").csv("wasbs://ididims@waginput.blob.core.windows.net/supplierdimcsv.csv")

// COMMAND ----------

val dfsupplier1 = dfsupplier.withColumn("Date", (col("eventdatetime").cast("date")))

// COMMAND ----------

dfsupplier1.write.format("delta").partitionBy("Date").save(deltapathsupplier)

// COMMAND ----------

val checkpointLocationlocation = "wasbs://deltaidi@waginput.blob.core.windows.net/deltaidi/delta_custdata/_checkpoints/etl-from-jsonlocation"
val deltapathlocation = "wasbs://deltaidi@waginput.blob.core.windows.net/deltaidi/delta_locationdim"

// COMMAND ----------

val dflocation = spark.read.option("header","true").option("inferSchema","true").csv("wasbs://ididims@waginput.blob.core.windows.net/locationdim.csv")

// COMMAND ----------

val dflocation1 = dflocation.withColumn("Date", (col("eventdatetime").cast("date")))

// COMMAND ----------

dflocation1.write.format("delta").partitionBy("Date").save(deltapathlocation)

// COMMAND ----------

val checkpointLocationteam = "wasbs://deltaidi@waginput.blob.core.windows.net/deltaidi/delta_custdata/_checkpoints/etl-from-jsonteam"
val deltapathteam = "wasbs://deltaidi@waginput.blob.core.windows.net/deltaidi/delta_teamdim"

// COMMAND ----------

val dfteam = spark.read.option("header","true").option("inferSchema","true").csv("wasbs://ididims@waginput.blob.core.windows.net/teamdim.csv")

// COMMAND ----------

val dfteam1 = dfteam.withColumn("Date", (col("eventdatetime").cast("date")))

// COMMAND ----------

dfteam1.write.format("delta").partitionBy("Date").save(deltapathteam)

// COMMAND ----------

