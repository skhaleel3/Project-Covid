package spark.project

import org.apache.spark.sql.functions.col
import spark.utils.SparkApp

object Covidtask4 extends SparkApp with App {

  val phu_locationsDf = spark.read.json("src/main/resources/Final_project_data/phu_locations.json")
  // phu_locationsDf.show(false)


  // Task 4: Find out number of PHUs are closed in each city on Friday

  val task4Df = phu_locationsDf.where(col("friday").isNull).groupBy("PHU","city").count()
  .show(100)
}
