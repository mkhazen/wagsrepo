// Databricks notebook source
dbutils.widgets.text("tableid", "10")

// COMMAND ----------

val tableid = dbutils.widgets.get("tableid")

// COMMAND ----------

print(tableid)

// COMMAND ----------



// COMMAND ----------

dbutils.notebook.exit(tableid)

// COMMAND ----------

