package fr.ippon.dojo.spark.exploration.rdd

import org.apache.spark.{SparkConf, SparkContext}

object ListDistinctJobs extends App {

  val conf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("list-distinct-jobs")
  val sc = new SparkContext(conf)

  val lines = sc.textFile("src/main/resources/bank-full.csv")
    .zipWithIndex()
    .filter(x => x._2 != 0)
    .map(x => x._1)

  val jobs = lines.map(x => x.split(";"))
    .map(x => (x(1).stripPrefix("\"").stripSuffix("\""), 0))

  val jobsDeduplicated = jobs.reduceByKey((a, b) => 0)
    .map(x => x._1)
    .sortBy(x => x)

  jobsDeduplicated.collect().foreach(x => println("Job: " + x))

}
