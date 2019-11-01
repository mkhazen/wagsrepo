// Databricks notebook source
dbutils.fs.unmount("/mnt/taxidata")
dbutils.fs.unmount("/mnt/deltalake")

// COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://incoming@waginput.blob.core.windows.net/",
  mountPoint = "/mnt/taxidata",
  extraConfigs = Map("fs.azure.account.key.waginput.blob.core.windows.net" -> "TBwLoPOLim87APX5grtZzWy8Td9h69F/BJgDxuiQyEP480Cs5zyOa2bUeVVRfUnCALOug3aA2Wb4cj8aIiqGEw=="))

// COMMAND ----------

display(dbutils.fs.ls("/mnt/taxidata"))

// COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://deltalake@waginput.blob.core.windows.net/",
  mountPoint = "/mnt/deltalake",
  extraConfigs = Map("fs.azure.account.key.waginput.blob.core.windows.net" -> "TBwLoPOLim87APX5grtZzWy8Td9h69F/BJgDxuiQyEP480Cs5zyOa2bUeVVRfUnCALOug3aA2Wb4cj8aIiqGEw=="))

// COMMAND ----------

dbutils.fs.ls("/mnt/deltalake/")

// COMMAND ----------

val dftest = spark.read.option("header","true").option("inferschema" , "true").csv("/mnt/taxidata/test.csv")

// COMMAND ----------

display(dftest)

// COMMAND ----------

val dftrain = spark.read.option("header","true").option("inferschema" , "true").csv("/mnt/taxidata/train.csv")

// COMMAND ----------

display(dftrain)

// COMMAND ----------

dftrain.createOrReplaceTempView("dftrain")

// COMMAND ----------

// MAGIC %sql 
// MAGIC 
// MAGIC DROP TABLE IF EXISTS taxidata_delta;

// COMMAND ----------

dbutils.fs.rm("dbfs:/mnt/deltalake/taxidata_delta/", true)

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC CREATE TABLE taxidata_delta
// MAGIC USING delta
// MAGIC LOCATION '/mnt/deltalake/taxidata_delta'
// MAGIC AS SELECT * FROM dftrain;
// MAGIC 
// MAGIC -- View Delta Lake table
// MAGIC SELECT * FROM taxidata_delta

// COMMAND ----------

dftrain.describe().show()

// COMMAND ----------

dftrain.printSchema

// COMMAND ----------

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors
val featureCols=Array("fare_amount","pickup_longitude","pickup_latitude","dropoff_longitude","dropoff_latitude","passenger_count")
val assembler: org.apache.spark.ml.feature.VectorAssembler= new VectorAssembler()
                .setInputCols(featureCols)
                .setOutputCol("features")

val assembledDF = assembler.setHandleInvalid("skip").transform(dftrain)
val assembledFinalDF = assembledDF.select("fare_amount","features")

// COMMAND ----------

display(assembledFinalDF)

// COMMAND ----------

import org.apache.spark.ml.feature.Normalizer

val normalizedDF = new Normalizer()
  .setInputCol("features")
  .setOutputCol("normalizedFeatures")
  .transform(assembledFinalDF)

// COMMAND ----------

display(normalizedDF.select("fare_amount","normalizedFeatures"))

// COMMAND ----------

val normalizedDF1 = normalizedDF.na.drop()

// COMMAND ----------

val Array(trainingDS, testDS) = normalizedDF1.randomSplit(Array(0.7, 0.3))

// COMMAND ----------

//trainingDS.cache
trainingDS.count

// COMMAND ----------

import org.apache.spark.ml.regression.LinearRegression
// Create a LinearRegression instance. This instance is an Estimator.
val lr = new LinearRegression()
// We may set parameters using setter methods.
            .setLabelCol("fare_amount")
            .setMaxIter(100)
// Print out the parameters, documentation, and any default values.
println(s"Linear Regression parameters:\n ${lr.explainParams()}\n")
// Learn a Linear Regression model. This uses the parameters stored in lr.
val lrModel = lr.fit(trainingDS)
// Make predictions on test data using the Transformer.transform() method.
// LinearRegression.transform will only use the 'features' column.
val lrPredictions = lrModel.transform(testDS)

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
println("\nPredictions : " )
	lrPredictions.select($"fare_amount".cast(IntegerType),$"prediction".cast(IntegerType)).orderBy(abs($"prediction"-$"fare_amount")).distinct.show(15)


// COMMAND ----------

display(lrModel, testDS, plotType="fittedVsResiduals")

// COMMAND ----------

import org.apache.spark.ml.evaluation.RegressionEvaluator
val evaluator_r2 = new RegressionEvaluator()
                .setPredictionCol("prediction")
                .setLabelCol("fare_amount")//The metric we select for evaluation is the coefficient of determination
                .setMetricName("r2")

//As the name implies, isLargerBetter returns if a larger value is better or smaller for evaluation.
val isLargerBetter : Boolean = evaluator_r2.isLargerBetter
println("Coefficient of determination = " + evaluator_r2.evaluate(lrPredictions))

// COMMAND ----------

//Evaluate the results. Calculate Root Mean Square Error
val evaluator_rmse = new RegressionEvaluator()
                .setPredictionCol("prediction")
                .setLabelCol("fare_amount")
//The metric we select for evaluation is RMSE
                .setMetricName("rmse")
//As the name implies, isLargerBetter returns if a larger value is better for evaluation.
val isLargerBetter1 : Boolean = evaluator_rmse.isLargerBetter
println("Root Mean Square Error = " + evaluator_rmse.evaluate(lrPredictions))

// COMMAND ----------

//import org.apache.spark.ml.regression.{RandomForestRegressionModel, RandomForestRegressor}
//import org.apache.spark.ml.evaluation.RegressionEvaluator

//Create the model
//val rf = new RandomForestRegressor()
//                    .setLabelCol("fare_amount")
//                    .setFeaturesCol("features")
//Number of trees in the forest
//                    .setNumTrees(100)
//Maximum depth of each tree in the forest
//                    .setMaxDepth(15)

//val rfModel = rf.fit(trainingDS)	
//Predict on the test data
//val rfPredictions = rfModel.transform(testDS)

// COMMAND ----------

//println("\nPredictions :")
//rfPredictions.select($"fare_amount".cast(IntegerType),$"prediction".cast(IntegerType)).orderBy(abs($"fare_amount"-$"prediction")).distinct.show(15)


// COMMAND ----------

//Destination directory to persist model
//val modelDirectoryPath = "/mnt/workshop/consumption/nyctaxi/model/duration-pred/" 
//Delete any residual data from prior executions for an idempotent run
//dbutils.fs.rm(destDataDirRoot,recurse=true)
// COMMAND ----------
//Persist model
//rfModel.save(modelDirectoryPath)