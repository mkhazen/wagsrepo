// Databricks notebook source
sc.version

// COMMAND ----------

dbutils.notebook.run("runstreams", 60, Map("tableid" -> "10"))

// COMMAND ----------

val list = (1 to 10).toList
list.par.map(_ + 0)

// COMMAND ----------

// define some way to generate a sequence of workloads to run
val jobArguments = list
 
// define the name of the Azure Databricks notebook to run
val notebookToRun = "runstreams"

// look up required context for parallel run calls
val context = dbutils.notebook.getContext()
 
// start the jobs
list.par.foreach(args => {
  // ensure thread knows about databricks context
  dbutils.notebook.setContext(context)
  //dbutils.notebook.run(notebookToRun, timeoutSeconds = 0, args.toString)
  dbutils.notebook.run(notebookToRun, timeoutSeconds = 0, Map("tableid" -> args.toString()))
})