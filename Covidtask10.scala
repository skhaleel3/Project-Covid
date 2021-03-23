package spark.project

import org.apache.spark.sql.functions._
import spark.utils.SparkApp

object Covidtask10 extends SparkApp with App {

  val cases_by_status_and_phuDf = spark.read.option("header","true").option("inferSchema","true").csv("src/main/resources/Final_project_data/cases_by_status_and_phu.csv")
  cases_by_status_and_phuDf.show(100)

// Task 10: which PHU has more resolved cases of covid-19 than active cases in year 2020

  val task10Df = cases_by_status_and_phuDf.selectExpr("PHU_NAME","ACTIVE_CASES","RESOLVED_CASES","FILE_DATE")
    .where(col("FILE_DATE").contains("2020"))
    .groupBy("PHU_NAME","RESOLVED_CASES","ACTIVE_CASES")
    .agg(
      max("FILE_DATE").as("Latest_Date")
    )
    .orderBy(col("Latest_Date")desc)
    .show(500)

}
