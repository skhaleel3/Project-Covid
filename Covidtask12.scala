package spark.project

import org.apache.spark.sql.functions._
import spark.utils.SparkApp


object Covidtask12 extends SparkApp with App {

  val cases_by_status_and_phuDf = spark.read.option("header","true").option("inferSchema","true").csv("src/main/resources/Final_project_data/cases_by_status_and_phu.csv")
 // cases_by_status_and_phuDf.show(false)

 // Task 12 : Find out total active cases remaining in each PHU

  cases_by_status_and_phuDf.createOrReplaceTempView("cases_by_status_and_phu")

  // val task12Df = cases_by_status_and_phuDf.groupBy("PHU_NAME").agg(sum("ACTIVE_CASES").as("Total active cases")).show()

val task12Df = cases_by_status_and_phuDf
    .select(col("PHU_NAME"),col("ACTIVE_CASES"),col("FILE_DATE"))
    .groupBy("PHU_NAME","ACTIVE_CASES")
    .agg(max("FILE_DATE")as "max")
    .orderBy(desc("max"))
    .show()
}
