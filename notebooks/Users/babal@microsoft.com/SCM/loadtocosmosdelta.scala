// Databricks notebook source
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

// COMMAND ----------

val custdatadf = spark.table("delta_custdata")

// COMMAND ----------

display(custdatadf)

// COMMAND ----------

custdatadf.count