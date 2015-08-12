package fr.ippon.spark.codingdojoml.classification

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * User: ludochane
 */
object Main {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("Spark coding dojo classification")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    val df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load("src/main/resources/")

    // feature engineering

  }
}
