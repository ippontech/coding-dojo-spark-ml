package fr.ippon.dojo.spark.utils

import java.sql.Date

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DateType
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime

/**
  * User: ludochane
  * Create the csv with the birthdate instead of age.
  */
object CreateCsvWithBirthDate {

   def main(args: Array[String]) {
     val conf = new SparkConf()
       .setAppName("Spark coding dojo classification with LogisticRegression")
       .setMaster("local[*]")

     val sc = new SparkContext(conf)

     val sqlContext = new SQLContext(sc)

     val df = sqlContext.read.format("com.databricks.spark.csv")
       .option("header", "true")
       .option("delimiter", ";")
       .option("inferSchema", "true")
       .load("src/main/resources/classification/bank-full.csv")


     val now: DateTime = new DateTime()

     val ageToDateBirth: (Int) => Date = age => new Date(now.minusYears(age).toDate().getTime())

     val birthDateDF = df.withColumn("birth_date", callUDF(ageToDateBirth, DateType, df("age"))).drop("age")
     //birthDateDF.printSchema()
     //birthDateDF.show(10)

     birthDateDF
       .write.format("com.databricks.spark.csv")
       .option("delimiter", ";")
       .option("header", "true")
       .save("src/main/resources/classification/bank-full-birthdate.csv")
   }
 }
