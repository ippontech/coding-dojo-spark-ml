package fr.ippon.dojo.spark.exploration.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

object WikipediaWorldcup extends App {

  val conf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("wikipedia-worldcup")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)


  // - prepare the schema for viewing stats
  // - join both dataframes on site and url columns
  // - select the "language", "pagename" and "views" columns
  // - group by the "language" and "views" columns
  // - aggregate the "views" using the org.apache.spark.sql.functions.sum() function
  // - sort the results
  // - print the results
  val viewsSchema = StructType(Array(
    StructField("site", DataTypes.StringType),
    StructField("url", DataTypes.StringType),
    StructField("views", DataTypes.LongType),
    StructField("size", DataTypes.LongType)))

  // - load the CSV file of viewing stats
  val views = sqlContext.read.format("com.databricks.spark.csv")
    .option("header", "false")
    .option("delimiter", " ")
    .option("quote", "\t")
    .schema(viewsSchema)
    .load("/Users/aseigneurin/dev/coding-dojo-spark/data/wikipedia-pagecounts-days/pagecounts-*")

  views.printSchema()
  views.show()

  // - register the dataframe as a table
  views.registerTempTable("views")


  // - prepare the schema for page names
  val pagesSchema = StructType(Array(
    StructField("site", DataTypes.StringType),
    StructField("language", DataTypes.StringType),
    StructField("pagename", DataTypes.StringType),
    StructField("url", DataTypes.StringType)))

  // - load the CSV file of page names
  val pages = sqlContext.read.format("com.databricks.spark.csv")
    .option("header", "false")
    .option("delimiter", "\t")
    .schema(pagesSchema)
    .load("/Users/aseigneurin/dev/coding-dojo-spark/data/wikipedia-worldcup-pages/worldcup-pages.txt")

  pages.printSchema()
  pages.show()

  // - register the dataframe as a table
  pages.registerTempTable("pages")


  // - perform the SQL request
  val res = sqlContext.sql(
    """select ...
      |from ...
    """.stripMargin)

  // - print the results
  res.show()

}
