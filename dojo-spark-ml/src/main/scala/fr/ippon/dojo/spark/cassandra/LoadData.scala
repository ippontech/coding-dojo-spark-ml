package fr.ippon.dojo.spark.cassandra

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object LoadData {

  def main(args: Array[String]): Unit = {

    val cassandraHost = "127.0.0.1"
    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("dope-data-loader")
      .set("spark.cassandra.connection.host", cassandraHost)
    implicit val sc = new SparkContext(sparkConf)
    implicit val sqlContext = new SQLContext(sc)

    val genresList = GenresLoader.loadGenres()

    RatingsLoader.loadRatings()

    MoviesLoader.loadMovies(genresList)

  }

}
