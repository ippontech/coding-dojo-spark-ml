package fr.ippon.dojo.spark.exploration

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext, functions}
import org.apache.spark.{SparkConf, SparkContext}

case class Tree(height: Float, specie: String)

object Trees extends App {

  val conf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("trees")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  // example 1: using RDDs
  sc.textFile("/Users/aseigneurin/dev/coding-dojo-spark/data/paris-arbresalignementparis2010/arbresalignementparis2010.csv")
    .filter(line => !line.startsWith("geom"))
    .map(line => line.split(";"))
    .map(fields => (fields(4), 1))
    .reduceByKey((x, y) => x + y)
    .sortByKey()
    .foreach(t => println(t._1 + " : " + t._2))

  // example 2: prepare data to be used with DataFrames
  val lines = sc.textFile("/Users/aseigneurin/dev/coding-dojo-spark/data/paris-arbresalignementparis2010/arbresalignementparis2010.csv")
    .zipWithIndex()
    .filter(x => x._2 != 0)
    .map(x => x._1)
    .map(_.split(";"))

  val trees1: RDD[Row] = lines.map(
    fields => Row(fields(7).toFloat, fields(3))
  )

  val trees2: RDD[Tree] = lines.map(
    fields => Tree(fields(7).toFloat, fields(3))
  )

  // example 3: DataFrames
  val df: DataFrame = sqlContext.createDataFrame(trees2)
  df
    .select("specie")
    .where(df("specie").notEqual(""))
    .groupBy("specie")
    .agg(functions.count("*"))
    .orderBy("specie")
    .show()

  // example 3: Spark SQL
  df.registerTempTable("tree")
  sqlContext.sql(
    """SELECT specie, COUNT(*)
      |FROM tree
      |WHERE specie <> ''
      |GROUP BY specie
      |ORDER BY specie""".stripMargin)
    .show()

}
