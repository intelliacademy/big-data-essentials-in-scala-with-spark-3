package com.intellibucket.lessons
package dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object DataSources_Playground extends App {

  var gCon = GConnection.as()

  var spark = SparkSession.builder()
    .appName("DataSources")
    .config("spark.master","local[2]")
    .getOrCreate()

  var personalSchema = StructType(
    Array(
      StructField("UUID",StringType),
      StructField("Name",StringType),
      StructField("Last name",StringType),
      StructField("Email",StringType),
      StructField("Sex",StringType),
      StructField("IP",StringType),
      StructField("Salary",DecimalType(7,2)),
      StructField("Currency",StringType),
      StructField("Country",StringType)
    )
  )

  var personalDF = spark.read
    .format("jdbc")
    .options(Map(
      "inferSchema"->"true",
      "driver"->"org.postgresql.Driver",
      "url"->gCon.url,
      "user"->gCon.username,
      "password"->gCon.password,
      "dbTable"->"internal.personals",
      "spark.sql.legacy.charVarcharAsString"->"true"
    ))
    .load()

  personalDF.take(10).foreach(println)

}
