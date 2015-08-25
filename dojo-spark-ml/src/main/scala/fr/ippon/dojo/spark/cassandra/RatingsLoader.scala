package fr.ippon.dojo.spark.cassandra

import com.datastax.spark.connector._
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SQLContext}

case class Rating(
                   user_id: Int,
                   movie_id: Int,
                   rating: Float,
                   ts: Long
                   )

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
    val ratingsDF = readRatings()

    val ratingsRDD = ratingsDF.map(row => Rating(
      user_id = row.getInt(0),
      movie_id = row.getInt(1),
      rating = row.getInt(2),
      ts = row.getLong(3)
    ))

    ratingsRDD.saveToCassandra("movielens", "ratings_by_movie")
    ratingsRDD.saveToCassandra("movielens", "ratings_by_user")
  }

}
