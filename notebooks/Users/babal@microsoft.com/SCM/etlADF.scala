// Databricks notebook source
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode

// COMMAND ----------

spark.conf.set(
  "fs.azure.account.key.waginput.blob.core.windows.net",
  "TBwLoPOLim87APX5grtZzWy8Td9h69F/BJgDxuiQyEP480Cs5zyOa2bUeVVRfUnCALOug3aA2Wb4cj8aIiqGEw==")

// COMMAND ----------

val df = spark.read.option("header","true").option("inferSchema","true").csv("wasbs://incoming@waginput.blob.core.windows.net/train.csv")
display(df)

// COMMAND ----------

df.count

// COMMAND ----------

val df1 = df.withColumn("Date", (col("pickup_datetime").cast("date")))
display(df1)

// COMMAND ----------

val df2 = df1.withColumn("year", year(col("date"))) .withColumn("month", month(col("date"))) .withColumn("day", dayofmonth(col("date"))) .withColumn("hour", hour(col("date")))

// COMMAND ----------

display(df2)

// COMMAND ----------

df2.groupBy("year","month").agg(mean("fare_amount").alias("mean")).show()

// COMMAND ----------

df2.groupBy("year","month").agg(sum("fare_amount").alias("Total"),count("key").alias("Count")).sort(asc("year"), asc("month")).show()