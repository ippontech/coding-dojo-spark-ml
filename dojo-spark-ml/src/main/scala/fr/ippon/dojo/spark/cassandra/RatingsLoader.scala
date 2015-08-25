package fr.ippon.dojo.spark.cassandra

import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{SaveMode, DataFrame, SQLContext}

object RatingsLoader {

  def readRatings()(implicit sc: SparkContext, sqlContext: SQLContext): DataFrame = {
    // user id | item id | rating | timestamp

    val schema = StructType(
      Array(
        StructField("user_id", DataTypes.IntegerType),
        StructField("movie_id", DataTypes.IntegerType),
        StructField("rating", DataTypes.IntegerType),
        StructField("ts", DataTypes.LongType)
      ))

    val df = sqlContext.read.format("com.databricks.spark.csv")
      .option("header", "false")
      .option("delimiter", "\t")
      .option("inferSchema", "false")
      .schema(schema)
      .load("src/main/resources/ml-100k/u.data")

    df.show()

    df
  }

  def loadRatings()(implicit sc: SparkContext, sqlContext: SQLContext) = {
    val ratings = readRatings()

    ratings.write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> "movielens", "table" -> "ratings_by_movie"))
      .mode(SaveMode.Append)
      .save()

    ratings.write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> "movielens", "table" -> "ratings_by_user"))
      .mode(SaveMode.Append)
      .save()
  }

}
