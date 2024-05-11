package com.intellibucket.lessons
package dataframes

import org.apache.spark.sql.SparkSession

object DataFrame1_Playground extends App {

  println("DataFrames")


  val spark = SparkSession.builder()
    .appName("Spark Playground")
    .config("spark.master","local")
    .getOrCreate()


  var options = Map(("inferSchema","true"))
  val personalsDataFrame = spark.read
    .format("csv")
    .options(options)
    .csv("src/main/resources/data/personals.csv")

  personalsDataFrame.show(10)
}
