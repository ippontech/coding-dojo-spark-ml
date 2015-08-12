package fr.ippon.spark.codingdojoml.utils

import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions
import org.apache.spark.ml.feature.{StandardScaler, OneHotEncoder, VectorAssembler, StringIndexer}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.types.DoubleType

/**
 * Created by Martin on 12/08/15.
 */
object FeatureEngineering {

  val catColsNames = List("job", "marital", "education", "default",
    "housing", "loan", "contact", "day_of_week", "poutcome")
  val numColsNames = List("age", "duration", "campaign", "pdays",
    "previous", "emp_var_rate", "cons_price_idx", "cons_conf_idx",
    "euribor3m", "nr_employed")

  def getMostFrequentCats(df: DataFrame): Map[String, String] = {
    this.catColsNames.map(c => (c,
      df.select(c)
        .map(r => r.getString(0))
        .countByValue()
        .maxBy(_._2)._1)).toMap[String, String]
  }

  def getMeans(df: DataFrame): Map[String, Double] = {
    val meanDf = df.agg(functions.mean(numColsNames.head).as(numColsNames.head),
      numColsNames.tail.map(c => functions.mean(c).as(c)):_*)
    meanDf.collect().head.getValuesMap[Double](numColsNames)
  }

  def fillNas(df: DataFrame): DataFrame = {
    var tempDF = df
    val mostFreqCats = getMostFrequentCats(df)
    val means = getMeans(df)

    catColsNames.foreach(c => tempDF = tempDF.na.replace(c, Map("unknown" -> mostFreqCats(c))))
    tempDF
  }

  def vectorizeNumCols(df: DataFrame): DataFrame = {
    var tempDF = df
    numColsNames.filter(c => c != "age")
      .foreach(c => tempDF = new VectorAssembler()
      .setInputCols(Array(c))
      .setOutputCol(c+"_vectorized").transform(tempDF))
    tempDF
  }

  def indexCatCols(df: DataFrame): DataFrame = {
    val pipeline = new Pipeline()
    val indexers = catColsNames.map{
        c => new StringIndexer()
          .setInputCol(c)
          .setOutputCol(c+"_indexed")
      }
    pipeline.setStages(indexers.toArray)
    pipeline.fit(df).transform(df)
  }

  def encodeCatCols(df: DataFrame): DataFrame = {
    val pipeline = new Pipeline()
    val encoders = catColsNames.map{
      c => new OneHotEncoder()
        .setInputCol(c+"_indexed")
        .setOutputCol(c+"_encoded")
    }
    pipeline.setStages(encoders.toArray)
    pipeline.fit(df).transform(df)
  }

  def scaleNumCols(df: DataFrame): DataFrame = {
    val pipeline = new Pipeline()
    val scalers = numColsNames
      .filter(c => c != "age")
      .map{
        c => new StandardScaler()
          .setInputCol(c+"_vectorized")
          .setOutputCol(c+"_scaled")
      }
    pipeline.setStages(scalers.toArray)
    val out = pipeline.fit(df).transform(df)
    out.show()
    out
  }

  def createFeatureCol(df: DataFrame): DataFrame = {
    val indexedDF = indexCatCols(df)
    val vectDF = vectorizeNumCols(indexedDF)
    val encodedDF = encodeCatCols(vectDF)
    val scaledDF = scaleNumCols(encodedDF)

    val va = new VectorAssembler()


    va.setInputCols((catColsNames.map(c => c+"_encoded") ++ numColsNames.filter(c => c != "age").map(c => c+"_scaled")).toArray)
    va.setOutputCol("features")
    va.transform(scaledDF).withColumn("age_label", df("age").cast(DoubleType))
  }
}
