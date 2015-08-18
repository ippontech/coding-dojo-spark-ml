package fr.ippon.dojo.spark.regression

import fr.ippon.dojo.spark.utils.FeatureEngineering
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.{GBTRegressor, LinearRegression}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}

object RegressionMain {

  val conf = new SparkConf()
    .setAppName("Spark coding dojo regression")
    .setMaster("local[*]")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  def main(args: Array[String]) {

    val df = sqlContext.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", ";")
      .option("inferSchema", "true")
      .load("src/main/resources/bank-full.csv")

    //////////////////////////////// Lab 1.1 - Linear Regression
    println("Lab 1.1 - Linear Regression")

    //////// Feature engineering

    // - replace the unknown values: FeatureEngineering.fillNas()
    val cleanedDF = FeatureEngineering.fillNas(df)

    // - standardize each feature and assemble a vector of features: FeatureEngineering.createFeatureCol()
    val featureDF = FeatureEngineering.createFeatureCol(cleanedDF)


    //////// Spliting in two dataset: training (80%) and test (20%)

    // - use df.randomSplit()
    val Array(training, prediction) = featureDF.randomSplit(Array(0.80, 0.20))


    //////// Train the model

    // - instantiate the algorithm: LinearRegression
    val lr = new LinearRegression()

    // - set the label column parameter on the algorithm: "age_label"
    lr.setLabelCol("age_label")

    // - fit the model on the training dataset: fit()
    val lrModel = lr.fit(training)


    //////// Apply the model

    // - apply the model on the test dataset: transform()
    val lrResult = lrModel.transform(prediction)

    // - compare the predictions with the expected results
    lrResult.withColumn("error", lrResult("age_label") - lrResult("prediction"))
      .select("age_label", "prediction", "error")
      .show()
    lrResult.withColumn("error", abs(lrResult("age_label") - lrResult("prediction")))
      .select("error")
      .agg(avg("error"))
      .show()


    //////////////////////////////// Lab 1.2 - Gradient Boosted Tree Regression
    println("Lab 1.2 - Gradient Boosted Tree Regression")

    //////// Train the model

    // - instantiate the algorithm: GBTRegressor
    val gbtr = new GBTRegressor()

    // - set the label column parameter on the algorithm: "age_label"
    gbtr.setLabelCol("age_label")

    // - set the "maxDepth" parameter (e.g. 10)
    gbtr.setMaxDepth(10)

    // - set the "stepSize" parameter (e.g. 0.01)
    gbtr.setStepSize(0.01)

    // - fit the model on the training dataset: fit()
    val gbtrModel = gbtr.fit(training)


    //////// Apply the model

    // - apply the model on the test dataset: transform()
    val gbtrResult = gbtrModel.transform(prediction)

    // - compare the predictions with the expected results
    gbtrResult.withColumn("error", gbtrResult("age_label") - gbtrResult("prediction"))
      .select("age_label", "prediction", "error")
      .show()
    gbtrResult.withColumn("error", abs(gbtrResult("age_label") - gbtrResult("prediction")))
      .select("error")
      .agg(avg("error"))
      .show()


    //////////////////////////////// Lab 1.3 - Grid Search & Cross Validation
    println("Lab 1.3 - Grid Search & Cross Validation")

    //////// Grid Search params

    // - create the grid of params: ParamGridBuilder
    val params = new ParamGridBuilder()

    // - set values for params:
    //   - maxDepth: 5, 10
    //   - stepSize: 0.01, 0.1
    params.addGrid(gbtr.maxDepth, Array(5, 10))
    params.addGrid(gbtr.stepSize, Array(0.01, 0.1))


    //////// Prediction evaluation using Root Mean Square

    // - instantiate an evaluator: RegressionEvaluator
    val evaluator = new RegressionEvaluator()

    // - set the label column parameter on the evaluator: "age_label"
    evaluator.setLabelCol("age_label")

    // - set the metric: "rmse"
    evaluator.setMetricName("rmse")


    //////// Cross Validation

    // - instantiate a cross validator: CrossValidator
    val cv = new CrossValidator()

    // - set the number of folds (e.g. 3)
    cv.setNumFolds(3)

    // - set the estimator (the ML algorithm)
    cv.setEstimator(gbtr)

    // - set the grid of params
    cv.setEstimatorParamMaps(params.build())

    // - set the evaluator (RMSE)
    cv.setEvaluator(evaluator)

    // - fit the model on the training dataset: fit()
    val cvModel = cv.fit(training)


    //////// Apply the model

    // - apply the model on the test dataset: transform()
    val cvResult = cvModel.transform(prediction)

    // - compare the precision with the previous results
    println("Grid search precision: " + evaluator.evaluate(cvResult))

    // - compare the predictions with the previous results
    cvResult.withColumn("error", cvResult("age_label") - cvResult("prediction"))
      .select("age_label", "prediction", "error")
      .show()
    cvResult.withColumn("error", abs(cvResult("age_label") - cvResult("prediction")))
      .select("error")
      .agg(avg("error"))
      .show()

    // - evaluate the RMSE precision of each of the 3 algorithms: evaluator.evaluate()
    println("Linear Regression precision: " + evaluator.evaluate(lrResult))
    println("Gradient boosted tree precision: " + evaluator.evaluate(gbtrResult))
    println("Cross validated model precision: " + evaluator.evaluate(cvResult))

  }
}
