package fr.ippon.spark.codingdojoml.dataExploration.rdd

import org.apache.spark.{SparkConf, SparkContext}

object AverageAgeByMaritalStatus extends App {

  val conf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("average-age-by-marital-status")
  val sc = new SparkContext(conf)

  val lines = sc.textFile("src/main/resources/bank-sample.csv")
    .zipWithIndex()
    .filter(x => x._2 != 0)
    .map(x => x._1)

  val ages = lines.map(x => x.split(";"))
    .map(x => (x(2).stripPrefix("\"").stripSuffix("\""), (x(0).toInt, 1)))

  val avgByMaritalStatus = ages.reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
    .mapValues(x => x._1 / x._2)

  avgByMaritalStatus.foreach(x => println("Average age for " + x._1 + "s: " + x._2))

}
