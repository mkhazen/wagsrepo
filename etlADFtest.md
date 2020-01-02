# Etl - Data engineering in Azure databricks

## use case

To simulate some data engineering or ETL based workload to process data in batch mode. The idea is to process millions of rows so the data set has 55 million rows. In future will add joins and other processing as well.

## Pre-requistie

```
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
```

## setup access to blob storage

```
spark.conf.set(
  "fs.azure.account.key.waginput.blob.core.windows.net",
  "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
```

## Load data

```
val df = spark.read.option("header","true").option("inferSchema","true").csv("wasbs://incoming@waginput.blob.core.windows.net/train.csv")
display(df)
```

## count the number of rows in the data set

```
df.count
```

Answer should be around: Long = 55423856

## Split date and time from a datetime stamp column

```
val df1 = df.withColumn("Date", (col("pickup_datetime").cast("date")))
display(df1)
```

The above is performed to store as year and month parition for long term batch processing.

## Split the year, month, day

```
val df2 = df1.withColumn("year", year(col("date"))) .withColumn("month", month(col("date"))) .withColumn("day", dayofmonth(col("date"))) .withColumn("hour", hour(col("date")))
```

display the data set

```
display(df2)
```

## do group by operation on year, month and day

```
df2.groupBy("year","month").agg(mean("fare_amount").alias("mean")).show()
```

```
df2.groupBy("year","month").agg(sum("fare_amount").alias("Total"),count("key").alias("Count")).sort(asc("year"), asc("month")).show()
```

## End

End of ETL or data engineering.