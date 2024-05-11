package com.intellibucket.lessons
package dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DateType, DoubleType, StringType, StructField, StructType}

object DataFrame1_Playground extends App {

  println("DataFrames")


  val spark = SparkSession.builder()
    .appName("Spark Playground")
    .config("spark.master","local")
    .getOrCreate()

  var newStructTypeForPersonal = StructType(
    Array(
      StructField("ID",StringType),
      StructField("Ad",StringType),
      StructField("Soyad",StringType),
      StructField("Elektron Unvan",StringType),
      StructField("Cins",StringType),
      StructField("Dogum gunu",StringType),
      StructField("Sheher",StringType),
      StructField("Unvan",StringType),
      StructField("Emek haqqi",DoubleType),
    )
  )

  var options = Map(("inferSchema","true"))
  val personalsDataFrame = spark.read
    .format("csv")
    .schema(newStructTypeForPersonal)
    .options(options)
    .csv("src/main/resources/data/personals.csv")



  personalsDataFrame.tail(10).foreach(println)

}
