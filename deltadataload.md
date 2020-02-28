# Loading dimension data into dimension table

Sample data is available in code\sampledata folder. 

File List:
locationdim.csv
productdim.csv
supplierdimcsv.csv
teamdim.csv

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


Now it is time to load the includes

```
    import scala.collection.JavaConverters._
    import com.microsoft.azure.eventhubs._
    import java.util.concurrent._
    import scala.collection.immutable._
    import scala.concurrent.Future
    import scala.concurrent.ExecutionContext.Implicits.global
import org.apache.spark.sql._ 
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions._

```

Config the ADLS store access

```
spark.conf.set(   "fs.azure.account.key.xxxxxxx.blob.core.windows.net", "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
````

Load Product Dimenstion:

```
val checkpointLocationproduct = "wasbs://xxxxx@xxxxx.blob.core.windows.net/deltaidi/delta_custdata/_checkpoints/etl-from-jsonproduct"
val deltapathproduct = "wasbs://xxxx@xxxx.blob.core.windows.net/deltaidi/delta_productdim"
```

```
val dfproduct = spark.read.option("header","true").option("inferSchema","true").csv("wasbs://xxxxx@xxxxxx.blob.core.windows.net/productdim.csv")
```

Create Parition Column

```
val dfproduct1 = dfproduct.withColumn("Date", (col("eventdatetime").cast("date")))
```

```
dfproduct1.write.format("delta").partitionBy("Date").save(deltapathproduct)
```

Load Supplier Dimenstion:

```
val checkpointLocationsupplier = "wasbs://xxxxx@xxxxx.blob.core.windows.net/deltaidi/delta_custdata/_checkpoints/etl-from-jsonsupplier"
val deltapathsupplier = "wasbs://xxxxxx@xxxxx.blob.core.windows.net/deltaidi/delta_supplierdim"
```

```
val dfsupplier = spark.read.option("header","true").option("inferSchema","true").csv("wasbs://xxxxx@xxxxxx.blob.core.windows.net/supplierdimcsv.csv")
```

Create Parition Column

```
val dfsupplier1 = dfsupplier.withColumn("Date", (col("eventdatetime").cast("date")))
```

```
dfsupplier1.write.format("delta").partitionBy("Date").save(deltapathsupplier)
```


Load Location Dimenstion:

```
val checkpointLocationlocation = "wasbs://xxxxx@xxxxx.blob.core.windows.net/deltaidi/delta_custdata/_checkpoints/etl-from-jsonlocation"
val deltapathlocation = "wasbs://xxxxxx@xxxxxx.blob.core.windows.net/deltaidi/delta_locationdim"
```

```
val dflocation = spark.read.option("header","true").option("inferSchema","true").csv("wasbs://xxxxxx@xxxxxx.blob.core.windows.net/locationdim.csv")
```

```
val dflocation1 = dflocation.withColumn("Date", (col("eventdatetime").cast("date")))
```

```
dflocation1.write.format("delta").partitionBy("Date").save(deltapathlocation)
```
Load Team Dimenstion:

```
val checkpointLocationteam = "wasbs://xxxxx@xxxxx.blob.core.windows.net/deltaidi/delta_custdata/_checkpoints/etl-from-jsonteam"
val deltapathteam = "wasbs://xxxxxx@xxxxx.blob.core.windows.net/deltaidi/delta_teamdim"
```

```
val dfteam = spark.read.option("header","true").option("inferSchema","true").csv("wasbs://xxxxx@xxxxx.blob.core.windows.net/teamdim.csv")
```

```
val dfteam1 = dfteam.withColumn("Date", (col("eventdatetime").cast("date")))
```

```
dfteam1.write.format("delta").partitionBy("Date").save(deltapathteam)
```

Now all the dimenstion tables should be loaded into dataframe to join as the stream is processing.