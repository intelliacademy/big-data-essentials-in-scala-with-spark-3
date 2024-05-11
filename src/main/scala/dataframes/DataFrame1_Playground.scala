package com.intellibucket.lessons
package dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
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
  val internal.personalsDataFrame = spark.read
    .format("csv")
    .schema(newStructTypeForPersonal)
    .options(options)
    .csv("src/main/resources/data/internal.personals.csv")

  var seqTaked10Rows = internal.personalsDataFrame
    .tail(10)
    .toSeq
    .map(row=>row.toSeq)


  var data1 = Seq(
    ("Samsung","S24 Ultra","Android",16),
    ("Samsung","S23 Ultra","Android",12),
    ("Apple","IPhone 15 Pro","IOS",15),
    ("Apple","IPhone 14 Pro","IOS",14)
  )

}
