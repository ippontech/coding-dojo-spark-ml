package fr.ippon.dojo.spark.classification

import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object ClassificationMain {

  val conf = new SparkConf()
    .setAppName("classification")
    .setMaster("local[*]")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  def main(args: Array[String]) {

    val df = sqlContext.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", ";")
      .option("inferSchema", "true")
      .load("src/main/resources/classification/bank-full-birthdate.csv")

    df.show()


    //////////////////////////////// Lab 2.1 - Logistic Regression
    println("Lab 2.1 - Logistic Regression")

    //////// Feature engineering

    // - calculate the age from the birth date: FeatureEngineering.calculateAge()

    // - use a StringIndexer to convert the label column ("y") to a Double column called "label"

    // - use StringIndexers to convert String columns to Double columns
    //   columns: "job", "marital", "education", "default", "housing", "loan"


    //////// Build the pipeline

    // - create a Transformer to transform numeric columns into a vector
    //   columns: "age", "jobIndex", "maritalIndex", "educationIndex", "defaultIndex", "housingIndex", "loanIndex", "duration"
    //   class: VectorAssembler

    // - create an Estimator and set its parameters (regParam=0.1, maxIter=50)
    //   class: LogisticRegression

    // - create a Pipeline and set the stages (VectorAssembler, LogisticRegression)
    //   class: Pipeline


    //////// Spliting in two dataset: training (80%) and test (20%)

    // - use df.randomSplit()


    //////// Train the model

    // - fit the model on the training dataset: fit()


    //////// Apply the model

    // - apply the model on the test dataset: transform()

    // - compare the predictions with the expected results


    //////////////////////////////// Lab 2.2 - Grid Search & Cross Validation
    println("Lab 2.2 - Grid Search & Cross Validation")

    //////// Grid Search params

    // - create the grid of params: ParamGridBuilder

    // - set values for params:
    //   - regParam: 1, 0.1, 0.01
    //   - maxIter: 10, 50, 100


    //////// Prediction evaluation using a binary evaluator

    // - instantiate an evaluator: BinaryClassificationEvaluator


    //////// Cross Validation

    // - instantiate a cross validator: CrossValidator

    // - set the number of folds (e.g. 3)

    // - set the estimator (the ML algorithm)

    // - set the grid of params

    // - set the evaluator

    // - fit the model on the training dataset: fit()


    //////// Apply the model

    // - apply the model on the test dataset: transform()

    // - compare the predictions with the expected results


    //////////////////////////////// Lab 2.3 - Random Forests
    println("Lab 2.3 - Random Forests")

    //////// Build the pipeline

    // - create an Estimator and set its parameters (...)
    //   class: RandomForestClassifier

    // - create a Pipeline and set the stages (VectorAssembler, RandomForestClassifier)
    //   class: Pipeline

    //////// Train the model

    // - fit the model on the training dataset: fit()


    //////// Apply the model

    // - apply the model on the test dataset: transform()

    // - compare the predictions with the expected results

  }
