package spark.project

import org.apache.spark.sql.functions._
import spark.utils.SparkApp

object Covidtask1 extends SparkApp with App {

  val phu_locationsDf = spark.read.json("src/main/resources/Final_project_data/phu_locations.json")
  phu_locationsDf.show(false)

  // Task 1 : list all PHU locations which are allow online appointments. List should not contain columns in French.
  // All French columns have name ending with fr

  val task1Df = phu_locationsDf.filter(col("online_appointments").isNotNull)
    .drop("PHU_fr","additional_information_fr","address_fr","location_name_fr","operated_by_fr")
    .show()





}
