package fr.ippon.dojo.spark.classification

import fr.ippon.dojo.spark.utils.FeatureEngineering
import org.apache.spark.ml.{PipelineModel, Pipeline}
import org.apache.spark.ml.classification.{LogisticRegression, RandomForestClassifier}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
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
    val dfAge = FeatureEngineering.calculateAge(df, "birth_date")

    // - use a StringIndexer to convert the label column ("y") to a Double column called "label"
    val dfYIndexed = new StringIndexer().setInputCol("y").setOutputCol("label").fit(dfAge).transform(dfAge)

    // - use StringIndexers to convert String columns to Double columns
    //   columns: "job", "marital", "education", "default", "housing", "loan"
    val dfJobIndexed = new StringIndexer().setInputCol("job").setOutputCol("jobIndex").fit(dfYIndexed).transform(dfYIndexed)
    val dfMaritalIndexed = new StringIndexer().setInputCol("marital").setOutputCol("maritalIndex").fit(dfJobIndexed).transform(dfJobIndexed)
    val dfEducationIndexed = new StringIndexer().setInputCol("education").setOutputCol("educationIndex").fit(dfMaritalIndexed).transform(dfMaritalIndexed)
    val dfDefaultIndexed = new StringIndexer().setInputCol("default").setOutputCol("defaultIndex").fit(dfEducationIndexed).transform(dfEducationIndexed)
    val dfHousingIndexed = new StringIndexer().setInputCol("housing").setOutputCol("housingIndex").fit(dfDefaultIndexed).transform(dfDefaultIndexed)
    val dfLoanIndexed = new StringIndexer().setInputCol("loan").setOutputCol("loanIndex").fit(dfHousingIndexed).transform(dfHousingIndexed)


    //////// Build the pipeline

    // - create a Transformer to transform numeric columns into a vector
    //   columns: "age", "jobIndex", "maritalIndex", "educationIndex", "defaultIndex", "housingIndex", "loanIndex", "duration"
    //   class: VectorAssembler
    val vectorAssembler = new VectorAssembler()
      .setInputCols(Array("age", "jobIndex", "maritalIndex", "educationIndex", "defaultIndex", "housingIndex", "loanIndex", "duration"))
      .setOutputCol("features")

    // - create an Estimator and set its parameters (regParam=0.1, maxIter=50)
    //   class: LogisticRegression
    val logisticRegression = new LogisticRegression()
      .setRegParam(0.1)
      .setMaxIter(100)

    // - create a Pipeline and set the stages (VectorAssembler, LogisticRegression)
    //   class: Pipeline
    val pipeline = new Pipeline().setStages(Array(
      vectorAssembler,
      logisticRegression
    ))


    //////// Spliting in two dataset: training (80%) and test (20%)

    // - use df.randomSplit()
    val Array(training, test) = dfLoanIndexed.randomSplit(Array(0.80, 0.20))


    //////// Train the model

    // - fit the model on the training dataset: fit()
    val lrModel = pipeline.fit(training)


    //////// Apply the model

    // - apply the model on the test dataset: transform()
    val lrResult = lrModel.transform(test)

    // - compare the predictions with the expected results
    lrResult
      .select("y", "label", "prediction")
      .groupBy("y", "label", "prediction")
      .agg(count("*"))
      .show()


    //////////////////////////////// Lab 2.2 - Grid Search & Cross Validation
    println("Lab 2.2 - Grid Search & Cross Validation")

    //////// Grid Search params

    // - create the grid of params: ParamGridBuilder
    val paramGrid = new ParamGridBuilder()

    // - set values for params:
    //   - regParam: 1, 0.1, 0.01
    //   - maxIter: 10, 50, 100
    paramGrid.addGrid(logisticRegression.regParam, Array(0.1, 0.03, 0.01))
    paramGrid.addGrid(logisticRegression.maxIter, Array(100, 150, 200))


    //////// Prediction evaluation using a binary evaluator

    // - instantiate an evaluator: BinaryClassificationEvaluator
    val evaluator = new BinaryClassificationEvaluator()


    //////// Cross Validation

    // - instantiate a cross validator: CrossValidator
    val crossValidator = new CrossValidator()

    // - set the number of folds (e.g. 3)
    crossValidator.setNumFolds(3)

    // - set the estimator (the ML algorithm)
    crossValidator.setEstimator(pipeline)

    // - set the grid of params
    crossValidator.setEstimatorParamMaps(paramGrid.build())

    // - set the evaluator
    crossValidator.setEvaluator(evaluator)

    // - fit the model on the training dataset: fit()
    val cvModel = crossValidator.fit(training)


    //////// Apply the model

    // - apply the model on the test dataset: transform()
    val cvResult = cvModel.transform(test)
    
    // - compare the predictions with the expected results
    cvResult
      .select("y", "label", "prediction")
      .groupBy("y", "label", "prediction")
      .agg(count("*"))
      .show()


    //////////////////////////////// Lab 2.3 - Random Forests
    println("Lab 2.3 - Random Forests")

    //////// Build the pipeline

    // - create an Estimator and set its parameters (...)
    //   class: RandomForestClassifier
    val randomForest = new RandomForestClassifier()

    // - create a Pipeline and set the stages (VectorAssembler, RandomForestClassifier)
    //   class: Pipeline
    val rfPipeline = new Pipeline().setStages(Array(
      vectorAssembler,
      randomForest
    ))

    //////// Train the model

    // - fit the model on the training dataset: fit()
    val rfModel = rfPipeline.fit(training)


    //////// Apply the model

    // - apply the model on the test dataset: transform()
    val rfResult = rfModel.transform(test)

    // - compare the predictions with the expected results
    rfResult
      .select("y", "label", "prediction")
      .groupBy("y", "label", "prediction")
      .agg(count("*"))
      .show()

  }
}
