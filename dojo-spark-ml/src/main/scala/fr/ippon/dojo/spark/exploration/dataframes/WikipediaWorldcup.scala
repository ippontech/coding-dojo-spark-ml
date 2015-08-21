package fr.ippon.dojo.spark.exploration.dataframes

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
    .load("src/main/resources/wikipedia-pagecounts-days/pagecounts-*")

  views.printSchema()
  views.show()


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
    .load("src/main/resources/wikipedia-worldcup-pages/worldcup-pages.txt")

  pages.printSchema()
  pages.show()


  // - join both dataframes on site and url columns
  val join = views.join(pages, views("site") === pages("site") && views("url") === pages("url"))

  // - select the "language", "pagename" and "views" columns

  // - group by the "language" and "views" columns

  // - aggregate the "views" using the org.apache.spark.sql.functions.sum() function

  // - sort the results

  // - print the results

}
