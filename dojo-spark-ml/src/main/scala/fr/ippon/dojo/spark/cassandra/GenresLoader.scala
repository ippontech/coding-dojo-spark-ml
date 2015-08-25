package fr.ippon.dojo.spark.cassandra

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

object GenresLoader {

  def loadGenres()(implicit sc: SparkContext, sqlContext: SQLContext): Array[String] = {
    // user id | item id | rating | timestamp

    val schema = StructType(
      Array(
        StructField("genre", DataTypes.StringType),
        StructField("genreId", DataTypes.IntegerType)
      ))

    val df = sqlContext.read.format("com.databricks.spark.csv")
      .option("header", "false")
      .option("delimiter", "|")
      .option("inferSchema", "false")
      .schema(schema)
      .load("src/main/resources/ml-100k/u.genre")

    df.show()

    df.map(row => row.getString(0))
      .collect()
  }

}
