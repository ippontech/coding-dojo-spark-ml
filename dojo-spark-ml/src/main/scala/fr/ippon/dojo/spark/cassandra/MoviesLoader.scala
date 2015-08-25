package fr.ippon.dojo.spark.cassandra

import com.datastax.spark.connector._
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext, SaveMode}

case class Movie(movie_id: Int,
                 title: String,
                 avg_rating: Double,
                 total_ratings: Int,
                 //release_date: String,
                 //video_release_date: String,
                 //imdb_url: String,
                 genres: Set[String])

object MoviesLoader {

  val IndexOfFirstGenre = 5

  def loadMovies(genresList: Array[String])(implicit sc: SparkContext, sqlContext: SQLContext) = {
    // movie id | movie title | release date | video release date |
    // IMDb URL | unknown | Action | Adventure | Animation |
    // Children's | Comedy | Crime | Documentary | Drama | Fantasy |
    // Film-Noir | Horror | Musical | Mystery | Romance | Sci-Fi |
    // Thriller | War | Western |

    val schema = StructType(
      Array(
        StructField("movie_id", DataTypes.IntegerType),
        StructField("title", DataTypes.StringType),
        StructField("release_date", DataTypes.StringType),
        StructField("video_release_date", DataTypes.StringType),
        StructField("imdb_url", DataTypes.StringType),

        StructField("genreUnknown", DataTypes.StringType),
        StructField("genreAction", DataTypes.StringType),
        StructField("genreAdventure", DataTypes.StringType),
        StructField("genreAnimation", DataTypes.StringType),
        StructField("genreChildren", DataTypes.StringType),
        StructField("genreComedy", DataTypes.StringType),
        StructField("genreCrime", DataTypes.StringType),
        StructField("genreDocumentary", DataTypes.StringType),
        StructField("genreDrama", DataTypes.StringType),
        StructField("genreFantasy", DataTypes.StringType),
        StructField("genreFilmNoir", DataTypes.StringType),
        StructField("genreHorror", DataTypes.StringType),
        StructField("genreMusical", DataTypes.StringType),
        StructField("genreMystery", DataTypes.StringType),
        StructField("genreRomance", DataTypes.StringType),
        StructField("genreSciFi", DataTypes.StringType),
        StructField("genreThriller", DataTypes.StringType),
        StructField("genreWar", DataTypes.StringType),
        StructField("genreWestern", DataTypes.StringType)
      ))

    val moviesDF = sqlContext.read.format("com.databricks.spark.csv")
      .option("header", "false")
      .option("delimiter", "|")
      .option("inferSchema", "false")
      .schema(schema)
      .load("src/main/resources/ml-100k/u.item")

    moviesDF.show()

    val ratingsDF = RatingsLoader.readRatings()
      .groupBy("movie_id")
      .agg(avg("rating").as("avg_rating"), sum("rating").as("total_ratings"))

    ratingsDF.show()

    val moviesWithRatingsDF = moviesDF.join(ratingsDF, "movie_id")

    moviesWithRatingsDF.show()


    val genresWithIndex: Array[(String, Int)] = genresList.zipWithIndex
      .map(t => (t._1, t._2 + IndexOfFirstGenre))
    val genresBroadcast: Broadcast[Array[(String, Int)]] = sc.broadcast(genresWithIndex)

    moviesWithRatingsDF.map(row => MoviesFunctions.createMovie(row, genresBroadcast))
      .saveToCassandra("movielens", "movies")

  }

}

object MoviesFunctions {

  def createMovie(row: Row, genresWithIndex: Broadcast[Array[(String, Int)]]): Movie = {

    val genres = genresWithIndex.value
      .map(t => (t._1, row.getString(t._2)))
      .filter(t => t._2 == "1")
      .map(t => t._1)
      .toSet

    Movie(row.getInt(0),
      row.getString(1),
      row.getDouble(row.length - 2),
      row.getLong(row.length - 1).toInt,
      genres
    )
  }

}
