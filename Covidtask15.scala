package spark.project

import org.apache.spark.sql.functions._
import spark.utils.SparkApp

object Covidtask15 extends SparkApp with App {

  val cases_by_status_and_phuDf = spark.read.option("header","true").option("inferSchema","true").csv("src/main/resources/Final_project_data/cases_by_status_and_phu.csv")
     // cases_by_status_and_phuDf.show(false)

  //  task 15: Find out percentage of active cases in each PHU in year 2020. Result should be like below

  val totalactiveDf= cases_by_status_and_phuDf.select(col("PHU_NAME"),col("ACTIVE_CASES"),col("FILE_DATE"))
    .where(col("FILE_DATE").startsWith("2020"))
    .groupBy("PHU_NAME","ACTIVE_CASES")
    .agg(
      max("FILE_DATE")as "max"
    )
    .orderBy(col("max")desc)

    val total = cases_by_status_and_phuDf.count()

      val Task15Df= totalactiveDf
        .select(col("PHU_NAME"), col("ACTIVE_CASES"))
        .withColumn("PERCENTAGE", col("ACTIVE_CASES") / total * 100)
          .show()

}
