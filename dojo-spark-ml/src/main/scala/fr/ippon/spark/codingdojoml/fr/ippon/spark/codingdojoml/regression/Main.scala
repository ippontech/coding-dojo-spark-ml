package fr.ippon.spark.codingdojoml.regression

import fr.ippon.spark.codingdojoml.utils.FeatureEngineering
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}

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
      .load("src/main/resources/bank-sample.csv")

    print(FeatureEngineering.mostFrequentCat(df))

    // feature engineering

  }
}
