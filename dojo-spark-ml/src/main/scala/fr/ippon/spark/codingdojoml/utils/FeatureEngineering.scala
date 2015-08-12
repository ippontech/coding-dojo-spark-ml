package fr.ippon.spark.codingdojoml.utils

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions

/**
 * Created by Martin on 12/08/15.
 */
object FeatureEngineering {

  val catColsNames = List("job", "marital", "education", "default",
    "housing", "loan", "contact", "day_of_week", "poutcome")
  val numColsNames = List("age", "duration", "campaign", "pdays",
    "previous", "emp_var_rate", "cons_price_idx", "cons_conf_idx",
    "euribor3m", "nr_employed")

  def getMostFrequentCats(df: DataFrame) = {
    this.catColsNames.map(c => (c,
      df.select(c)
        .map(_(0))
        .countByValue()
        .maxBy(_._2)._1)).toMap
  }

  def getMeans(df: DataFrame) = {
    val meanDf = df.agg(functions.mean(numColsNames.head).as(numColsNames.head),
      numColsNames.tail.map(c => functions.mean(c).as(c)):_*)
    meanDf.collect().head.getValuesMap(numColsNames)
  }
}
