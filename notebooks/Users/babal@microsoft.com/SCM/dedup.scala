// Databricks notebook source
spark.conf.set(
  "fs.azure.account.key.waginput.blob.core.windows.net",
  "TBwLoPOLim87APX5grtZzWy8Td9h69F/BJgDxuiQyEP480Cs5zyOa2bUeVVRfUnCALOug3aA2Wb4cj8aIiqGEw==")

// COMMAND ----------

val df = spark.read.option("header","true").option("inferSchema","true").csv("wasbs://incoming@waginput.blob.core.windows.net/train.csv")

// COMMAND ----------

display(df)

// COMMAND ----------

df.count()

// COMMAND ----------

df.printSchema

// COMMAND ----------

df.dropDuplicates("key","pickup_datetime","pickup_longitude","pickup_latitude","dropoff_longitude","dropoff_latitude")

// COMMAND ----------

df.count()