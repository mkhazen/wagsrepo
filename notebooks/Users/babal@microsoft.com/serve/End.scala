// Databricks notebook source
dbutils.widgets.text("tableid", "10")
val tableid = dbutils.widgets.get("tableid")

// COMMAND ----------

print(tableid)