package spark.project

import org.apache.spark.sql.functions.col
import spark.utils.SparkApp

object Covidtask6 extends SparkApp with App {

  val phu_locationsDf = spark.read.json("src/main/resources/Final_project_data/phu_locations.json")
  // phu_locationsDf.show(false)


  // Task 6 : List the hours of operation of all the PHU in Toronto. Use below format.

  val task6Df = phu_locationsDf
    .select("PHU","monday","tuesday","wednesday","thursday","friday","saturday","sunday")
    .where(col("city") === "Toronto")
    .withColumnRenamed("monday","Monday")
    .withColumnRenamed("tuesday","Tuesday")
    .withColumnRenamed("wednesday","Wednesday")
    .withColumnRenamed("thursday","Thursday")
    .withColumnRenamed("friday","Friday")
    .withColumnRenamed("saturday","Saturday")
    .withColumnRenamed("sunday","Sunday")

task6Df.na.drop().show()
}
