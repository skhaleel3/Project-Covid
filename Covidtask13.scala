package spark.project

import org.apache.spark.sql.functions._
import spark.utils.SparkApp


object Covidtask13 extends SparkApp with App {

  val cases_by_status_and_phuDf = spark.read.option("header","true").option("inferSchema","true").csv("src/main/resources/Final_project_data/cases_by_status_and_phu.csv")
 // cases_by_status_and_phuDf.show(false)

 //Task13: Find out on which date highest and lowest death reported in “NIAGARA REGION” PHU

  val MinDDf = cases_by_status_and_phuDf
    .select("FILE_DATE", "PHU_NAME", "DEATHS")
    .filter(col("PHU_NAME") === "NIAGARA REGION")
    .groupBy("FILE_DATE")
    .agg(
      min("DEATHS").as ("DEATHS")
    )
    .orderBy(col("DEATHS").asc).limit(1)

  val MaxDDf = cases_by_status_and_phuDf
    .select("FILE_DATE", "PHU_NAME", "DEATHS")
    .filter(col("PHU_NAME") === "NIAGARA REGION")
    .groupBy("FILE_DATE")
    .agg(
      max("DEATHS").as( "DEATHS")
    )
    .orderBy(col("DEATHS").desc).limit(1)

  val Task13Df = MinDDf union MaxDDf
  Task13Df.show()

}
