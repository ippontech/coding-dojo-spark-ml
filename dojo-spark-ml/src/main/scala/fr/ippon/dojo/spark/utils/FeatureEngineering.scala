package fr.ippon.dojo.spark.utils

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{OneHotEncoder, StandardScaler, StringIndexer, VectorAssembler}
import org.apache.spark.sql.{DataFrame, functions}
import org.apache.spark.sql.types.{DoubleType, IntegerType}
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, Years}

/**
 * Created by Martin on 12/08/15.
 */
object FeatureEngineering {

  val catColsNames = List("job", "marital", "education", "default",
    "housing", "loan", "contact", "day_of_week", "poutcome", "y")
  val numColsNames = List("age", "duration", "campaign", "pdays",
    "previous", "emp_var_rate", "cons_price_idx", "cons_conf_idx",
    "euribor3m", "nr_employed")

  // Retrieve the most represented categories within the categorical columns
  def getMostFrequentCats(df: DataFrame): Map[String, String] = {
    this.catColsNames.map(c => (c,
      df.select(c)
        .map(r => r.getString(0))
        .countByValue()
        .maxBy(_._2)._1)).toMap[String, String]
  }

  // Retrieve the means within the numerical columns
  def getMeans(df: DataFrame): Map[String, Double] = {
    val meanDf = df.agg(functions.mean(numColsNames.head).as(numColsNames.head),
      numColsNames.tail.map(c => functions.mean(c).as(c)): _*)
    meanDf.collect().head.getValuesMap[Double](numColsNames)
  }

  /*
   Use the two methods above to fill the Not Assigned values.
   We only process the cat columns here because the numerical columns have no NA values
  */
  def fillNas(df: DataFrame): DataFrame = {
    var tempDF = df
    val mostFreqCats = getMostFrequentCats(df)
    val means = getMeans(df)

    catColsNames.foreach(c => tempDF = tempDF.na.replace(c, Map("unknown" -> mostFreqCats(c))))
    tempDF
  }

  // Index categorical columns
  def indexCatCols(df: DataFrame): DataFrame = {
    val pipeline = new Pipeline()
    val indexers = catColsNames.map {
      c => new StringIndexer()
        .setInputCol(c)
        .setOutputCol(c + "_indexed")
    }
    pipeline.setStages(indexers.toArray)
    pipeline.fit(df).transform(df)
  }

  // One hot encode indexed columns
  def encodeCatCols(df: DataFrame): DataFrame = {
    val indexedDF = indexCatCols(df)
    val pipeline = new Pipeline()
    val encoders = catColsNames.map {
      c => new OneHotEncoder()
        .setInputCol(c + "_indexed")
        .setOutputCol(c + "_encoded")
    }
    pipeline.setStages(encoders.toArray)
    pipeline.fit(indexedDF).transform(indexedDF)
  }

  // Vector the numerical feature to be able to scale them (it usually improves a regression)
  def vectorizeNumCols(df: DataFrame): DataFrame = {
    var tempDF = df
    numColsNames.filter(c => c != "age")
      .foreach(c => tempDF = new VectorAssembler()
      .setInputCols(Array(c))
      .setOutputCol(c + "_vectorized").transform(tempDF))
    tempDF
  }

  // Scale the numerical values ((value - mean) / standard deviation)
  def scaleNumCols(df: DataFrame): DataFrame = {
    val vectorizedDF = vectorizeNumCols(df)
    val pipeline = new Pipeline()
    val scalers = numColsNames
      .filter(c => c != "age")
      .map {
      c => new StandardScaler()
        .setInputCol(c + "_vectorized")
        .setOutputCol(c + "_scaled")
        .setWithMean(true)
        .setWithStd(true)
    }
    pipeline.setStages(scalers.toArray)
    pipeline.fit(vectorizedDF).transform(vectorizedDF)
  }

  // Assemble all the feature cols in one Vector column "feature"
  def createFeatureCol(df: DataFrame): DataFrame = {
    val encodedDF = encodeCatCols(df)
    val scaledDF = scaleNumCols(encodedDF)

    val va = new VectorAssembler()

    /*
     Here we create a Vectorized feature with the cat columns encoded and the scaled numerical columns without the age (which is our target)
    */
    va.setInputCols((catColsNames.map(c => c + "_encoded") ++
      numColsNames.filter(c => c != "age").map(c => c + "_scaled")).toArray)
    va.setOutputCol("features")
    va.transform(scaledDF).withColumn("age_label", df("age").cast(DoubleType))
  }

  // Adds
  def calculateAge(df: DataFrame, birthDateColumn: String): DataFrame = {
    import org.apache.spark.sql.functions._
    val now = new DateTime()
    val birthToAge: (String) => Int = birthDate => {
      val formatter = DateTimeFormat.forPattern("yyyy-MM-dd")
      Years.yearsBetween(formatter.parseDateTime(birthDate), now).getYears
    }
    df.withColumn("age", callUDF(birthToAge, IntegerType, df(birthDateColumn)))
  }
}
