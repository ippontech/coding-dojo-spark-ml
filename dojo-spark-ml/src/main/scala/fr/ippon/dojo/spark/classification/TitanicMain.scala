package fr.ippon.dojo.spark.classification

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StringType}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object TitanicMain {

  val conf = new SparkConf()
    .setAppName("classification")
    .setMaster("local[*]")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  def main(args: Array[String]) {

    // load the training CSV file using Spark CSV
    val rawTraining = sqlContext.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", ",")
      .option("inferSchema", "true")
      .load("src/main/resources/titanic/train.csv")

    rawTraining.show()

    // load the test CSV file using Spark CSV
    val rawTest = sqlContext.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", ",")
      .option("inferSchema", "true")
      .load("src/main/resources/titanic/test.csv")

    rawTest.show()

    // load the expected results CSV file using Spark CSV
    val expected = sqlContext.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", ",")
      .option("inferSchema", "true")
      .load("src/main/resources/titanic/gendermodel.csv")

    expected.show()

    //////// Data cleansing

    // - calculate the mean age
    //   use the calcMeanAge() function
    val meanAge = calcMeanAge(rawTraining, "Age")

    // - fill in missing "Age" values with the mean age (training+test dataset)
    //   -> use the fillMissingAge() function


    //////// Feature engineering

    // - use a StringIndexer to convert the label column ("Survived") to a numeric column called "label" (training data)
    //   -> call the fit() method then the transform method()

    // - create a StringIndexer to transform the "Sex" column to a numeric column "Sex_indexed"


    //////// Build the pipeline

    // - create a VectorAssembler to transform numeric columns into a vector
    //   columns: "Pclass", "Sex_indexed", "Age"

    // - create an Estimator and set its parameters (...)
    //   class: RandomForestClassifier

    // - create a Pipeline and set the stages (StringIndexer, VectorAssembler, RandomForestClassifier)
    //   class: Pipeline


    //////// Train the model

    // - fit the model on the training dataset: fit()

    // - optional: display the model
    //println(model.asInstanceOf[PipelineModel].stages(4).asInstanceOf[RandomForestClassificationModel].toDebugString)

    //////// Apply the model

    // - apply the model on the test dataset: transform()

    // - compare the predictions with the expected results
    //    predictions
    //      .join(expected, "PassengerId")
    //      .select("Survived", "prediction")
    //      .groupBy("Survived", "prediction")
    //      .agg(count("*"))
    //      .show()


    //////// Optional: try adding a feature

    // - calculate the most frequent "Embarked" value
    //val mostFrequentEmbarked = calcMostFrequentEmbarked(rawTraining, "Embarked")

    // - fill in missing "Embarked" values with the most frequent value (training+test dataset)
    //val trainingTemp2 = fillMissingEmbarked(trainingTemp1, "Embarked", "Embarked_cleaned", mostFrequentEmbarked)
    //val testTemp2 = fillMissingEmbarked(testTemp1, "Embarked", "Embarked_cleaned", mostFrequentEmbarked)

    // - create a StringIndexer to transform the "Embarked_cleaned" column to a numeric column "Embarked_indexed"
    //val embarkedIndexer = new StringIndexer().setInputCol("Embarked_cleaned").setOutputCol("Embarked_indexed")

    // - create a OneHotEncoder to transform the "Embarked_indexed" column to a vector column "Embarked_encoded"
    //val embarkedEncoder = new OneHotEncoder().setInputCol("Embarked_indexed").setOutputCol("Embarked_encoded")

    // - add the "Embarked_encoded" column to the VectorAssembler

    // - train, apply the model, and compare the results



  }

  def calcMeanAge(df: DataFrame, inputCol: String): Double = {
    df.agg(avg(inputCol))
      .head()
      .getDouble(0)
  }

  def fillMissingAge(df: DataFrame, inputCol: String, outputCol: String, replacementValue: Double): DataFrame = {
    val ageOrMeanAge: (Any) => Double = age => age match {
      case age: Double => age
      case _ => replacementValue
    }

    df.withColumn(outputCol, callUDF(ageOrMeanAge, DoubleType, df(inputCol)))
  }

  def calcMostFrequentEmbarked(df: DataFrame, inputCol: String): String = {
    val embarkedCounts = df.groupBy(inputCol)
      .agg(count(inputCol).as("count"))
    embarkedCounts.orderBy(embarkedCounts("count").desc)
      .head()
      .getString(0)
  }

  def fillMissingEmbarked(df: DataFrame, inputCol: String, outputCol: String, replacementValue: String): DataFrame = {
    val embarkedOrMostFrequentEmbarked: (String) => String = embarked => embarked match {
      case "" => replacementValue
      case _ => embarked
    }

    df.withColumn(outputCol, callUDF(embarkedOrMostFrequentEmbarked, StringType, df(inputCol)))
  }

}
