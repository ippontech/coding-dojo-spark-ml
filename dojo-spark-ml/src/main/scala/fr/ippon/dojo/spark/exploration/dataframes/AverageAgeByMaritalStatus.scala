package fr.ippon.dojo.spark.exploration.dataframes

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}

object AverageAgeByMaritalStatus extends App {

  val conf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("average-age-by-marital-status")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  // - load the CSV file
  val lines = sqlContext.read.format("com.databricks.spark.csv")
    .option("header", "true")
    .option("delimiter", ";")
    .option("inferSchema", "true")
    .load("src/main/resources/bank-sample.csv")

  lines.printSchema()
  lines.show()

  // - group by the "martial" column

  // - aggregate the age using the org.apache.spark.sql.functions.avg() function

  // - print the results

}
