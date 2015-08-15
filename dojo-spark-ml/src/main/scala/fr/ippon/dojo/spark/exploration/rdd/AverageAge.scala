package fr.ippon.dojo.spark.exploration.rdd

import org.apache.spark.{SparkConf, SparkContext}

object AverageAge extends App {

  val conf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("average-age")
  val sc = new SparkContext(conf)

  val lines = sc.textFile("src/main/resources/bank-sample.csv")
    .zipWithIndex()
    .filter(x => x._2 != 0)
    .map(x => x._1)

  val ages = lines.map(x => x.split(";"))
    .map(x => x(0).toInt)

  val avg = ages.sum() / ages.count()

  println("Average age: " + avg)

}
