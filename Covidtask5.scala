package spark.project

import org.apache.spark.sql.functions._
import spark.utils.SparkApp

object Covidtask5 extends SparkApp with App {

  val phu_locationsDf = spark.read.json("src/main/resources/Final_project_data/phu_locations.json")
   //.show(false)

  // Task 5: find out all the PHU locations which are open during 20:00 and 21:00 on Monday in Toronto city

    val task5Df = phu_locationsDf.select("PHU","city","monday")
      .filter(col("city").contains("Toronto"))
      .withColumn("part_one", split(col("monday"), "-")(0))
      .withColumn("part_two", split(col("monday"), "-").getItem(1))
      .withColumn("start_time", split(trim(col("part_one")), ":").getItem(0))
      .withColumn("end_time", split(trim(col("part_two")),":").getItem(0))
      .where(col("start_time") <= 20 and (col("end_time") >= 21 or col("end_time") === 0))
      .drop("part_one","part_two","start_time","end_time")
      .show(1500)





}
