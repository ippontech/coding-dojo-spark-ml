package fr.ippon.dojo.spark.exploration.rdd

import org.apache.spark.{SparkConf, SparkContext}

object AverageAge extends App {

  val conf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("average-age")
  val sc = new SparkContext(conf)

  // - load the file ("src/main/resources/bank-full.csv")
  // val lines = sc....

  // - skip the first line

  // - split the lines by the ";" char

  // - extract the age and convert the string to int

  // - sum and count the ages, and divide one by the other

  // - print the result

}
