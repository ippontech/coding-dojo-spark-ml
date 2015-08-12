package fr.ippon.spark.codingdojoml.classification

import org.apache.spark.ml.{PipelineModel, Pipeline}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.tuning.{ParamGridBuilder, CrossValidator}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * User: ludochane
 */
object Main {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("Spark coding dojo classification")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    val df = sqlContext.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", ";")
      .option("inferSchema", "true")
      .load("src/main/resources/bank-full.csv")

    // printSchema
    df.printSchema()

    // show sample data
    df.show(10)

    // feature engineering


    // machine learning
    // We separate our data : trainSet = 75% of our data, validationSet = 25% of our data
    val Array(trainSet, validationSet) = df.randomSplit(Array(0.75, 0.25))

    // Pipeline
    val jobIndexer = new StringIndexer().setInputCol("job").setOutputCol("jobIndex")
    val maritalIndexer = new StringIndexer().setInputCol("marital").setOutputCol("maritalIndex")
    val housingIndexer = new StringIndexer().setInputCol("housing").setOutputCol("housingIndex")
    val loanIndexer = new StringIndexer().setInputCol("loan").setOutputCol("loanIndex")
    val yIndexer = new StringIndexer().setInputCol("y").setOutputCol("label")
    val vectorAssembler = new VectorAssembler()
      .setInputCols(Array("age", "jobIndex", "maritalIndex", "housingIndex", "loanIndex", "duration"))
      .setOutputCol("features")
    val logisticRegression = new LogisticRegression()

    val pipeline = new Pipeline().setStages(Array(
      jobIndexer,
      maritalIndexer,
      housingIndexer,
      loanIndexer,
      yIndexer,
      vectorAssembler,
      logisticRegression
    ))

    // We will cross validate our pipeline
    val crossValidator = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(new BinaryClassificationEvaluator)

    // Here are the params we want to validationPredictions
    val paramGrid = new ParamGridBuilder()
      .addGrid(logisticRegression.regParam, Array(1, 0.1, 0.01))
      .addGrid(logisticRegression.maxIter, Array(10, 50, 100))
      .build()
    crossValidator.setEstimatorParamMaps(paramGrid)

    // We will use a 3-fold cross validation
    crossValidator.setNumFolds(3)

    println("Cross Validation")
    val cvModel = crossValidator.fit(trainSet)

    println("Best model")
    for (stage <- cvModel.bestModel.asInstanceOf[PipelineModel].stages) println(stage.explainParams())

    println("Evaluate the model on the validation set.")
    val validationPredictions = cvModel.transform(validationSet)

    // We want to print the percentage of passengers we correctly predict on the validation set
    val total = validationPredictions.count()
    val goodPredictionCount = validationPredictions.filter(validationPredictions("label") === validationPredictions("prediction")).count()
    println(s"correct prediction percentage : ${goodPredictionCount / total.toDouble}")
  }
}
