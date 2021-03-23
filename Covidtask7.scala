package spark.project

import org.apache.spark.sql.functions._
import spark.utils.SparkApp

object Covidtask7 extends SparkApp with App {

  val phu_locationsDf = spark.read.json("src/main/resources/Final_project_data/phu_locations.json")
  phu_locationsDf.show(false)

  //  Task 7 : Count all the PHU are temporary closed in each city.

  val task7Df = phu_locationsDf
    .where(col("temporarily_closed") === "Yes")
    .groupBy("PHU","city").count()
    .show()
}
