package fr.ippon.spark.codingdojoml.regression

import fr.ippon.spark.codingdojoml.utils.FeatureEngineering
import org.apache.spark.ml.regression.{GBTRegressor, LinearRegression}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.tuning.{ParamGridBuilder, CrossValidator}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * User: mmenestret
 */
object Main {
  val conf = new SparkConf()
    .setAppName("Spark coding dojo regression")
    .setMaster("local[*]")

  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  def main(args: Array[String]) {

    val df = sqlContext.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", ";")
      .option("inferSchema", "true")
      .load("src/main/resources/bank-full.csv")

    // feature engineering
    val cleanedDF = FeatureEngineering.fillNas(df)
    val featureDF = FeatureEngineering.createFeatureCol(cleanedDF)

    // Spliting in two dataset: training and prediction
    val Array(training, prediction) = featureDF.randomSplit(Array(0.80, 0.20))

    // LinearRegressor
    val lr = new LinearRegression().setLabelCol("age_label")
    val lrModel = lr.fit(training)
    val lrResult = lrModel.transform(prediction)

    // Gradient Boosted Tree Regressor
    val gbtr = new GBTRegressor().setLabelCol("age_label")
    val gbtrModel = gbtr.fit(training)
    val gbtrResult = gbtrModel.transform(prediction)

    // Prediction evaluation using root mean square
    val evaluator = new RegressionEvaluator()
      .setLabelCol("age_label")
      .setMetricName("rmse")

    println(s"Gradient boosted tree precision: ${evaluator.evaluate(gbtrResult)}")
    println(s"Linear Regression precision: ${evaluator.evaluate(lrResult)}")

    // CrossValidation
    val params = new ParamGridBuilder()
      .addGrid(gbtr.maxDepth, Array(5, 10))
      .addGrid(gbtr.stepSize, Array(0.01, 0.1))
      .build()

    val cv = new CrossValidator()
    val cvModel = cv.setNumFolds(3)
      .setEstimator(gbtr)
      .setEstimatorParamMaps(params)
      .setEvaluator(evaluator)
      .fit(training)
    val cvResult = cvModel.transform(prediction)

    println(s"Grid search precision: ${evaluator.evaluate(cvResult)}")
  }
}
