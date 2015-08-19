package fr.ippon.dojo.spark.exploration.rdd

import org.apache.spark.{SparkConf, SparkContext}

object CountByJob extends App {

  val conf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("count-by-job")
  val sc = new SparkContext(conf)

  val lines = sc.textFile("src/main/resources/bank-sample.csv")
    .zipWithIndex()
    .filter(x => x._2 != 0)
    .map(x => x._1)

  val ages = lines.map(x => x.split(";"))
    .map(x => (x(1).stripPrefix("\"").stripSuffix("\""), (x(0).toInt, 1)))

  val countByJob = ages.reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
    .mapValues(x => x._1 / x._2)

  countByJob.foreach(x => println("Count for job " + x._1 + ": " + x._2))

}
