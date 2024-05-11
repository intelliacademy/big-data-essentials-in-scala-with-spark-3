package com.intellibucket.lessons
package dataframes

import org.apache.spark.sql.types.{DateType, DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{SaveMode, SparkSession}

object DataFrame2_Playground extends App {

  println("DataFrames")

  var carsSchema = StructType(
    Array(
      StructField("ID",StringType),
      StructField("Ad",StringType),
      StructField("Soyad",StringType),
      StructField("Elektron Unvan",StringType),
      StructField("Cins",StringType),
      StructField("Dogum gunu",DateType),
      StructField("Sheher",StringType),
      StructField("Unvan",StringType),
      StructField("Emek haqqi",DoubleType),
    )
  )

  val spark = SparkSession.builder()
    .appName("Spark Playground")
    .config("spark.master","local")
    .getOrCreate()


  var carsDF = spark.read
    .format("json")
    .options(Map(
      "inferSchema"->"true",
      "mode" -> "failFast",
      "path"->"src/main/resources/data/cars.json",
      "dateFormat"->"YYYY-MM-DD",
      "allowSingleQuotes"->"true",
      "compression"->"uncompressed"
    ))
    .load()

  carsDF.take(10).foreach(println)

  carsDF.write
    .mode(SaveMode.Overwrite)
    .parquet("src/main/resources/data/cars.cars")


}
