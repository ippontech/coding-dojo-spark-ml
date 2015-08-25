package fr.ippon.dojo.spark.cassandra

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext, SaveMode}

object MoviesLoader {

  val IndexOfFirstGenre = 5

  def loadMovies(genresList: Array[String])(implicit sc: SparkContext, sqlContext: SQLContext) = {
    // movie id | movie title | release date | video release date |
    // IMDb URL | unknown | Action | Adventure | Animation |
    // Children's | Comedy | Crime | Documentary | Drama | Fantasy |
    // Film-Noir | Horror | Musical | Mystery | Romance | Sci-Fi |
    // Thriller | War | Western |


    // --- load the CSV file

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

      .drop("release_date")
      .drop("video_release_date")
      .drop("imdb_url")

    moviesDF.show()


    // --- convert the boolean columns of the genres into a set of genre names

    val genresBroadcast: Broadcast[Array[String]] = sc.broadcast(genresList)

    sqlContext.udf.register("getGenres",
      (genreUnknown: String, genreAction: String, genreAdventure: String, genreAnimation: String,
       genreChildren: String, genreComedy: String, genreCrime: String, genreDocumentary: String,
       genreDrama: String, genreFantasy: String, genreFilmNoir: String, genreHorror: String,
       genreMusical: String, genreMystery: String, genreRomance: String, genreSciFi: String,
       genreThriller: String, genreWar: String, genreWestern: String)
      => getGenres(genreUnknown, genreAction, genreAdventure, genreAnimation,
        genreChildren, genreComedy, genreCrime, genreDocumentary,
        genreDrama, genreFantasy, genreFilmNoir, genreHorror,
        genreMusical, genreMystery, genreRomance, genreSciFi,
        genreThriller, genreWar, genreWestern,
        genresBroadcast))

    val moviesWithGenresDF = moviesDF
      .withColumn("genres",
        callUdf("getGenres",
          moviesDF("genreUnknown"),
          moviesDF("genreAction"),
          moviesDF("genreAdventure"),
          moviesDF("genreAnimation"),
          moviesDF("genreChildren"),
          moviesDF("genreComedy"),
          moviesDF("genreCrime"),
          moviesDF("genreDocumentary"),
          moviesDF("genreDrama"),
          moviesDF("genreFantasy"),
          moviesDF("genreFilmNoir"),
          moviesDF("genreHorror"),
          moviesDF("genreMusical"),
          moviesDF("genreMystery"),
          moviesDF("genreRomance"),
          moviesDF("genreSciFi"),
          moviesDF("genreThriller"),
          moviesDF("genreWar"),
          moviesDF("genreWestern")
        ))
      .drop("genreUnknown")
      .drop("genreAction")
      .drop("genreAdventure")
      .drop("genreAnimation")
      .drop("genreChildren")
      .drop("genreComedy")
      .drop("genreCrime")
      .drop("genreDocumentary")
      .drop("genreDrama")
      .drop("genreFantasy")
      .drop("genreFilmNoir")
      .drop("genreHorror")
      .drop("genreMusical")
      .drop("genreMystery")
      .drop("genreRomance")
      .drop("genreSciFi")
      .drop("genreThriller")
      .drop("genreWar")
      .drop("genreWestern")


    // --- incorporate the ratings

    val ratingsDF = RatingsLoader.readRatings()
      .groupBy("movie_id")
      .agg(avg("rating").as("avg_rating"), sum("rating").as("total_ratings"))

    ratingsDF.show()

    val moviesWithGenresAndRatings = moviesWithGenresDF.join(ratingsDF, "movie_id")

    moviesWithGenresAndRatings.show()


    // --- save to Cassandra

    moviesWithGenresAndRatings.write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> "movielens", "table" -> "movies"))
      .mode(SaveMode.Append)
      .save()

  }

  def getGenres(genreUnknown: String,
                genreAction: String,
                genreAdventure: String,
                genreAnimation: String,
                genreChildren: String,
                genreComedy: String,
                genreCrime: String,
                genreDocumentary: String,
                genreDrama: String,
                genreFantasy: String,
                genreFilmNoir: String,
                genreHorror: String,
                genreMusical: String,
                genreMystery: String,
                genreRomance: String,
                genreSciFi: String,
                genreThriller: String,
                genreWar: String,
                genreWestern: String,
                genresBroadcast: Broadcast[Array[String]]): Seq[String] = {

    val bools = Seq(genreUnknown, genreAction, genreAdventure, genreAnimation, genreChildren, genreComedy,
      genreCrime, genreDocumentary, genreDrama, genreFantasy, genreFilmNoir, genreHorror, genreMusical,
      genreMystery, genreRomance, genreSciFi, genreThriller, genreWar, genreWestern)
      .map(s => if (s == "1") true else false)

    genresBroadcast.value.zip(bools)
      .filter(_._2)
      .map(_._1)
  }

}
