package spark.project

import org.apache.spark.sql.functions._
import spark.utils.SparkApp


object Covidtask14 extends SparkApp with App {

  val cases_by_status_and_phuDf = spark.read.option("header","true").option("inferSchema","true").csv("src/main/resources/Final_project_data/cases_by_status_and_phu.csv")
   cases_by_status_and_phuDf .show()

  //question 14:Find out total resolved cases of covid-19 in “NORTH BAY PARRY SOUND DISTRICT” in month May 2020 and Oct 2020.

  val Task14Df = cases_by_status_and_phuDf
    .selectExpr("PHU_NAME","FILE_DATE", "RESOLVED_CASES","substring(FILE_DATE,0,4) as Year", "substring(FILE_DATE,5,2) as Month","substring(FILE_DATE,7,2) as Day")
    .where("Month== '05' or Month =='10'")
    .where(col("Year") === "2020")
    .where(col("PHU_NAME") === "NORTH BAY PARRY SOUND DISTRICT")
    .groupBy("Month")
    .agg(
      sum("RESOLVED_CASES").as( "Total Resolved Cases")
    )
    .show()

}
