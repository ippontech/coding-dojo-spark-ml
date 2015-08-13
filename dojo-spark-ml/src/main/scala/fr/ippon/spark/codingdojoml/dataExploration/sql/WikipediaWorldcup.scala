package fr.ippon.spark.codingdojoml.dataExploration.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

object WikipediaWorldcup extends App {

  val conf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("wikipedia-worldcup")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)


  val viewsSchema = StructType(Array(
    StructField("site", DataTypes.StringType),
    StructField("url", DataTypes.StringType),
    StructField("views", DataTypes.LongType),
    StructField("size", DataTypes.LongType)))

  val views = sqlContext.read.format("com.databricks.spark.csv")
    .option("header", "false")
    .option("delimiter", " ")
    .option("quote", "\t")
    .schema(viewsSchema)
    .load("/Users/aseigneurin/dev/coding-dojo-spark/data/wikipedia-pagecounts-days/pagecounts-*")

  views.printSchema()
  views.show()
  views.registerTempTable("views")


  val pagesSchema = StructType(Array(
    StructField("site", DataTypes.StringType),
    StructField("language", DataTypes.StringType),
    StructField("pagename", DataTypes.StringType),
    StructField("url", DataTypes.StringType)))

  val pages = sqlContext.read.format("com.databricks.spark.csv")
    .option("header", "false")
    .option("delimiter", "\t")
    .schema(pagesSchema)
    .load("/Users/aseigneurin/dev/coding-dojo-spark/data/wikipedia-worldcup-pages/worldcup-pages.txt")

  pages.printSchema()
  pages.show()
  pages.registerTempTable("pages")


  val res = sqlContext.sql(
    """select language, pagename, sum(views) as v
      |from views
      |join pages on views.site = pages.site and views.url = pages.url
      |group by language, pagename
      |order by v desc
    """.stripMargin)

  res.show()

}
