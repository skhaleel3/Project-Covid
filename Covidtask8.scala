package spark.project

import org.apache.spark.sql.functions._
import spark.utils.SparkApp

object Covidtask8 extends SparkApp with App {

  val phu_locationsDf = spark.read.json("src/main/resources/Final_project_data/phu_locations.json")
   phu_locationsDf.show(1000)

//   Task 8 : phu_locations.json has phone field. Create two more columns. First field should contain only
//   area code and 2nd field should contain extension number.

   val task8Df = phu_locationsDf
     .filter(col("phone").contains("ext"))
     .select(col("phone"), substring(col("phone"),1,3).as("area_code"), substring( col("phone"),19,4).as("extension"))
     .show(100)








}
