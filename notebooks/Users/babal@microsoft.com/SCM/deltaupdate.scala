// Databricks notebook source
//https://github.com/microsoft/Azure-Databricks-NYC-Taxi-Workshop/blob/master/code/03-Data-Science/scala/02-model-training.scala

// COMMAND ----------

val dftrain = spark.read.option("header","true").option("inferschema" , "true").csv("/mnt/taxidata/train.csv")

// COMMAND ----------

dftrain.createOrReplaceTempView("updates")

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC MERGE INTO taxidata_delta
// MAGIC using updates 
// MAGIC ON taxidata_delta.pickup_longitude = updates.pickup_longitude AND taxidata_delta.pickup_latitude = updates.pickup_latitude
// MAGIC  AND taxidata_delta.dropoff_longitude = updates.dropoff_longitude AND taxidata_delta.dropoff_latitude = updates.dropoff_latitude
// MAGIC  AND taxidata_delta.passenger_count = updates.passenger_count 
// MAGIC WHEN MATCHED THEN
// MAGIC  UPDATE *
// MAGIC WHEN NOT MATCHED 
// MAGIC  THEN INSERT *