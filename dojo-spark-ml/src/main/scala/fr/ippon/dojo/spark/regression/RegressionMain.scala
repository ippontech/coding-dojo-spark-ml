package fr.ippon.dojo.spark.regression

import org.apache.spark.sql.SQLContext
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

    df.show()

    //////////////////////////////// Lab 1.1 - Linear Regression

    //////// Feature engineering

    // - replace the unknown values: FeatureEngineering.fillNas()

    // - standardize each feature and assemble a vector of features: FeatureEngineering.createFeatureCol()


    //////// Spliting in two dataset: training (80%) and test (20%)

    // - use featureDF.randomSplit()


    //////// Train the model

    // - instantiate the algorithm: LinearRegression

    // - set the label column parameter on the algorithm: "age_label"

    // - fit the model on the training dataset: fit()


    //////// Apply the model

    // - apply the model on the test dataset: transform()

    // - compare the predictions with the expected results


    //////////////////////////////// Lab 1.2 - Gradient Boosted Tree Regression

    //////// Train the model

    // - instantiate the algorithm: GBTRegressor

    // - set the label column parameter on the algorithm: "age_label"

    // - fit the model on the training dataset: fit()


    //////// Apply the model

    // - apply the model on the test dataset: transform()

    // - compare the predictions with the expected results


    //////////////////////////////// Lab 1.3 - Grid Search & Cross Validation

    //////// Grid Search params

    // - create the grid of params: ParamGridBuilder

    // - set values for params:
    //   - maxDepth: 5, 10
    //   - stepSize: 0.01, 0.1


    //////// Prediction evaluation using Root Mean Square

    // - instantiate an evaluator: RegressionEvaluator

    // - set the label column parameter on the evaluator: "age_label"

    // - set the metric: "rmse"

    // - evaluate the precision: evaluate()


    //////// Cross Validation

    // - instantiate a cross validator: CrossValidator

    // - set the number of folds (e.g. 3)

    // - set the estimator (the ML algorithm)

    // - set the grid of params

    // - set the evaluator (RMSE)

    // - fit the model on the training dataset: fit()


    //////// Apply the model

    // - apply the model on the test dataset: transform()

    // - compare the predictions with the previous results

  }
}
