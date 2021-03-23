package spark.project

import org.apache.spark.sql.functions._
import spark.utils.SparkApp

object Covidtask3 extends SparkApp with App {

  val phu_locationsDf = spark.read.json("src/main/resources/Final_project_data/phu_locations.json")
  // phu_locationsDf.show(false)

  // Task 3: List all the PHU locations in Toronto has free parking with drive through.

  val task3Df = phu_locationsDf
    .filter(col("PHU").contains("Toronto"))
    .where(col("free_parking").contains("Yes"))
    .show(100)

}
