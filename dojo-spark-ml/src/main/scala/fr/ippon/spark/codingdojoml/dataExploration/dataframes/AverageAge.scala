package fr.ippon.spark.codingdojoml.dataExploration.dataframes

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}

object AverageAge extends App {

  val conf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("average-age")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  val lines = sqlContext.read.format("com.databricks.spark.csv")
    .option("header", "true")
    .option("delimiter", ";")
    .option("inferSchema", "true")
    .load("src/main/resources/bank-sample.csv")

  lines.printSchema()
  lines.show()

  val avgAge = lines.agg(avg("age"))
    .show()

}
