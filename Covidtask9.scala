package spark.project


import org.apache.spark.sql.functions._
import spark.utils.SparkApp

object Covidtask9 extends SparkApp with App {

  val cases_by_status_and_phuDf = spark.read.option("header","true").option("inferSchema","true").csv("src/main/resources/Final_project_data/cases_by_status_and_phu.csv")
   cases_by_status_and_phuDf.show(1000)

// Task 9 : Find out highest number of resolved cases of covid-19 in each PHU between April 2020 to Sep
  //2020

  val task9Df = cases_by_status_and_phuDf
    .selectExpr("PHU_NAME","RESOLVED_CASES","FILE_DATE","substring(FILE_DATE,0,4) as Year","substring(FILE_DATE,5,2) as Month")
    .where(col("month").between("04","09"))
    .where(col("year") === "2020")
    .groupBy(col("PHU_NAME"))//,col("Month"),col("Year"))
    .agg(
      max("RESOLVED_CASES").as("Max of Resolved Cases"),
   min("RESOLVED_CASES").as("Min of Resolved Cases")
    )
    .orderBy(col("Max of Resolved Cases").desc)
    .withColumn("Highest resolved",col("Max of Resolved Cases")- col("Min of Resolved Cases"))
    .drop("Max of Resolved Cases").drop("Min of Resolved Cases")
    .show(100)

}
