package spark.project

import org.apache.spark.sql.functions._
import spark.utils.SparkApp

object Covidtask2 extends SparkApp with App {

  val phu_locationsDf = spark.read.json("src/main/resources/Final_project_data/phu_locations.json")
  // phu_locationsDf.show(false)


  // Task 2 : List all the PHU locations allow children under 2 in Brampton location.

  val task2Df = phu_locationsDf
    .where(col("city") === "Brampton" and col("children_under_2") === "Yes")
    .show()
}
