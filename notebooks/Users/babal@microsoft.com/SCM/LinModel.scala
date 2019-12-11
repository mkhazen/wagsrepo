// Databricks notebook source
spark.conf.set(
  "fs.azure.account.key.waginput.blob.core.windows.net",
  "TBwLoPOLim87APX5grtZzWy8Td9h69F/BJgDxuiQyEP480Cs5zyOa2bUeVVRfUnCALOug3aA2Wb4cj8aIiqGEw==")

// COMMAND ----------

val df = spark.read.option("header","true").option("inferSchema","true").csv("wasbs://incoming@waginput.blob.core.windows.net/train.csv")

// COMMAND ----------

display(df)

// COMMAND ----------

df.printSchema

// COMMAND ----------

df.head(10)

// COMMAND ----------

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors
val featureCols=Array("fare_amount","pickup_longitude","pickup_latitude","dropoff_longitude","dropoff_latitude","passenger_count")
val assembler: org.apache.spark.ml.feature.VectorAssembler= new VectorAssembler().setInputCols(featureCols).setOutputCol("features")

val assembledDF = assembler.setHandleInvalid("skip").transform(df)
val assembledFinalDF = assembledDF.select("fare_amount","features")

// COMMAND ----------

import org.apache.spark.ml.feature.Normalizer

val normalizedDF = new Normalizer().setInputCol("features").setOutputCol("normalizedFeatures").transform(assembledFinalDF)

// COMMAND ----------

val normalizedDF1 = normalizedDF.na.drop()

// COMMAND ----------

val Array(trainingDS, testDS) = normalizedDF1.randomSplit(Array(0.7, 0.3))

// COMMAND ----------

trainingDS.count

// COMMAND ----------

import org.apache.spark.ml.regression.LinearRegression
// Create a LinearRegression instance. This instance is an Estimator.
val lr = new LinearRegression().setLabelCol("fare_amount").setMaxIter(100)
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

import org.apache.spark.ml.evaluation.RegressionEvaluator
val evaluator_r2 = new RegressionEvaluator().setPredictionCol("prediction").setLabelCol("fare_amount").setMetricName("r2")

//As the name implies, isLargerBetter returns if a larger value is better or smaller for evaluation.
val isLargerBetter : Boolean = evaluator_r2.isLargerBetter
println("Coefficient of determination = " + evaluator_r2.evaluate(lrPredictions))

// COMMAND ----------

//Evaluate the results. Calculate Root Mean Square Error
val evaluator_rmse = new RegressionEvaluator().setPredictionCol("prediction").setLabelCol("fare_amount").setMetricName("rmse")
//As the name implies, isLargerBetter returns if a larger value is better for evaluation.
val isLargerBetter1 : Boolean = evaluator_rmse.isLargerBetter
println("Root Mean Square Error = " + evaluator_rmse.evaluate(lrPredictions))