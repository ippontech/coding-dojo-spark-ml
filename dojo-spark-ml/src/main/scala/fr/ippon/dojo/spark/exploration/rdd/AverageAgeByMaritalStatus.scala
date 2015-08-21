package fr.ippon.dojo.spark.exploration.rdd

import org.apache.spark.{SparkConf, SparkContext}

object AverageAgeByMaritalStatus extends App {

  val conf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("average-age-by-marital-status")
  val sc = new SparkContext(conf)

  // - load the CSV file ("src/main/resources/bank-full.csv")
  // val lines = sc...

  // - skip the header line

  // - extract the marital status column

  // - put the marital statuses in a tuple with, as value, a tuple with the age and a counter initialized at 1

  // - use a reduce-by-key operation to sum and count the ages corresponding to the same keys

  // - print the results

}
