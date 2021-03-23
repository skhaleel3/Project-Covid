package spark.project

import org.apache.spark.sql.functions._
import spark.utils.SparkApp


object Covidtask11 extends SparkApp with App {

  val cases_by_status_and_phuDf = spark.read.option("header","true").option("inferSchema","true").csv("src/main/resources/Final_project_data/cases_by_status_and_phu.csv")
   cases_by_status_and_phuDf
    // .show(false)

  // Task11: List only PHU which has least death due to covid-19 in each month.

  val task11Df = cases_by_status_and_phuDf
    .groupBy(col("PHU_NAME"), col("FILE_DATE"))
    .agg(
      sum("DEATHS").as("LEAST DEATHS")
    )
    .orderBy(min("DEATHS"))
    .show()

}
