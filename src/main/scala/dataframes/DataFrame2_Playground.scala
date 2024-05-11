package com.intellibucket.lessons
package dataframes

import org.apache.spark.sql.{SaveMode, SparkSession}

object DataFrame2_Playground extends App {

  println("DataFrames")

  val spark = SparkSession.builder()
    .appName("Spark Playground")
    .config("spark.master","local")
    .getOrCreate()


  var carsDF = spark.read
    .format("json")
    .options(Map(
      "inferSchema"->"true",
      "mode" -> "failFast",
      "path"->"src/main/resources/data/cars.json"
    ))
    .load()

  carsDF.write
    .format("csv")
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/cars")

}
