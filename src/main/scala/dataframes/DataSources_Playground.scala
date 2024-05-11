package com.intellibucket.lessons
package dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DecimalType, StringType, StructField, StructType, VarcharType}

object DataSources_Playground extends App {

  var spark = SparkSession.builder()
    .appName("DataSources")
    .config("spark.master","local[2]")
    .getOrCreate()

  var personalDF = spark.read
    .options(Map(
      "mode"->"failFast",
      "infernSchame"->"true"
    ))

  var personalSchema = StructType(
    Array(
      StructField("UUID",VarcharType(40)),
      StructField("Name",StringType),
      StructField("Last name",StringType),
      StructField("Email",StringType),
      StructField("Sex",VarcharType(10)),
      StructField("IP",StringType),
      StructField("Salary",DecimalType(1,2)),
      StructField("Currency",VarcharType(10)),
      StructField("Country",VarcharType(10))
    )
  )
}
