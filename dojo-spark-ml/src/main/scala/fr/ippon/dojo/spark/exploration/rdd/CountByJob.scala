package fr.ippon.dojo.spark.exploration.rdd

import org.apache.spark.{SparkConf, SparkContext}

object CountByJob extends App {

  val conf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("count-by-job")
  val sc = new SparkContext(conf)

  // - load the CSV file ("src/main/resources/bank-full.csv")
  // val lines = sc...

  // - skip the header line

  // - extract the job column

  // - put the jobs in a tuple with the value 1

  // - use a reduce-by-key operation to sum values corresponding to the same keys

  // - print the results

}
