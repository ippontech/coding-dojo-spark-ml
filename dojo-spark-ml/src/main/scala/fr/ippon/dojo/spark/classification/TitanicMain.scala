package fr.ippon.dojo.spark.classification

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.feature._
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
    val meanAge = calcMeanAge(rawTraining, "Age")

    // - calculate the most frequent "Embarked" value
    val mostFrequentEmbarked = calcMostFrequentEmbarked(rawTraining, "Embarked")

    // - fill in missing "Age" values with the mean age (training+test dataset)
    val trainingTemp1 = fillMissingAge(rawTraining, "Age", "Age_cleaned", meanAge)
    val testTemp1 = fillMissingAge(rawTest, "Age", "Age_cleaned", meanAge)

    // - fill in missing "Embarked" values with the most frequent value (training+test dataset)
    val trainingTemp2 = fillMissingEmbarked(trainingTemp1, "Embarked", "Embarked_cleaned", mostFrequentEmbarked)
    val testTemp2 = fillMissingEmbarked(testTemp1, "Embarked", "Embarked_cleaned", mostFrequentEmbarked)


    val training = trainingTemp2
    val test = testTemp2

    //////// Feature engineering

    // - use a StringIndexer to convert the label column ("Survived") to a numeric column called "label"
    val labelIndexerModel = new StringIndexer()
      .setInputCol("Survived")
      .setOutputCol("label")
      .fit(training)
    val trainingWithLabel = labelIndexerModel.transform(training)

    // - create a StringIndexer to transform the "Sex" column to a numeric column "Sex_indexed"
    val sexIndexer = new StringIndexer().setInputCol("Sex").setOutputCol("Sex_indexed")

    // - create a StringIndexer to transform the "Embarked_cleaned" column to a numeric column "Embarked_indexed"
    val embarkedIndexer = new StringIndexer().setInputCol("Embarked_cleaned").setOutputCol("Embarked_indexed")

    // - create a OneHotEncoder to transform the "Embarked_indexed" column to a vector column "Embarked_encoded"
    val embarkedEncoder = new OneHotEncoder().setInputCol("Embarked_indexed").setOutputCol("Embarked_encoded")


    //////// Build the pipeline

    // - create a VectorAssembler to transform numeric columns into a vector
    //   columns: "Pclass", "Sex_indexed", "Age", "Embarked_indexed"
    val vectorAssembler = new VectorAssembler()
      .setInputCols(
        Array(
          "Pclass", "Sex_indexed", "Age_cleaned"
          //FIXME: "Pclass", "Sex_indexed", "Age_cleaned", "Fare", "Embarked_encoded"
        ))
      .setOutputCol("features")

    // - create an Estimator and set its parameters (...)
    //   class: RandomForestClassifier
    val randomForest = new RandomForestClassifier()
      .setNumTrees(30)

    // - create a Pipeline and set the stages (StringIndexers, VectorAssembler, RandomForestClassifier)
    //   class: Pipeline
    val pipeline = new Pipeline()
      .setStages(
        Array(
          sexIndexer,
          embarkedIndexer,
          embarkedEncoder,
          vectorAssembler,
          randomForest
        ))


    //////// Train the model

    // - fit the model on the training dataset: fit()
    val model = pipeline.fit(trainingWithLabel)

    // - optional: display the model
    //println(model.asInstanceOf[PipelineModel].stages(4).asInstanceOf[RandomForestClassificationModel].toDebugString)

    //////// Apply the model

    // - apply the model on the test dataset: transform()
    val predictions = model.transform(test)

    // - compare the predictions with the expected results
    predictions
      .join(expected, "PassengerId")
      .select("Survived", "prediction")
      .groupBy("Survived", "prediction")
      .agg(count("*"))
      .show()


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
