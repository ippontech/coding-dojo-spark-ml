package fr.ippon.spark.codingdojoml.regression

import fr.ippon.spark.codingdojoml.utils.FeatureEngineering
import org.apache.spark.ml.regression.{GBTRegressor, LinearRegression}
import org.apache.spark.ml.evaluation.RegressionEvaluator
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

    val Array(training, prediction) = featureDF.randomSplit(Array(0.75, 0.25))

    val lr = new LinearRegression().setLabelCol("age_label")
    val lrModel = lr.fit(training)
    val lrResult = lrModel.transform(prediction)

    val gbtr = new GBTRegressor().setLabelCol("age_label")
    val gbtrModel = gbtr.fit(training)
    val gbtrResult = gbtrModel.transform(prediction)

    val evaluator = new RegressionEvaluator()
      .setLabelCol("age_label")
      .setMetricName("rmse")
    println(s"Gbtr precision: ${evaluator.evaluate(gbtrResult)}")
    println(s"lr precision: ${evaluator.evaluate(lrResult)}")
  }
}
