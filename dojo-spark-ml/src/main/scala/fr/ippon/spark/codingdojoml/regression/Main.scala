package fr.ippon.spark.codingdojoml.regression

import fr.ippon.spark.codingdojoml.utils.FeatureEngineering
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
      .load("src/main/resources/bank-sample.csv")

    println(FeatureEngineering.getMostFrequentCats(df))
    println(FeatureEngineering.getMeans(df))

    // feature engineering

  }
}
