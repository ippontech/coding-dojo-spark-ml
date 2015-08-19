package fr.ippon.dojo.spark.exploration.rdd

import org.apache.spark.{SparkConf, SparkContext}

object CountByJob extends App {

  val conf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("count-by-job")
  val sc = new SparkContext(conf)

  val lines = sc.textFile("src/main/resources/bank-full.csv")
    .zipWithIndex()
    .filter(x => x._2 != 0)
    .map(x => x._1)

  val jobs = lines.map(x => x.split(";"))
    .map(x => (x(1).stripPrefix("\"").stripSuffix("\""), 1))

  val countByJob = jobs.reduceByKey(_ + _)
    .sortBy(x => -x._2)
    .collect()

  countByJob.foreach(x => println(x._1 + " => " + x._2))

}
