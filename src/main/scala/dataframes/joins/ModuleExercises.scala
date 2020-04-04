package dataframes.joins

import java.util.Properties

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

object ModuleExercises extends App {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  private val sparkSession: SparkSession = SparkSession.builder
    .appName("DataFrame - Joins")
    .config("spark.master", "local")
    .getOrCreate

  private val properties: Properties = new Properties
  properties.setProperty("user", "docker")
  properties.setProperty("password", "docker")
  properties.setProperty("password", "docker")
  properties.setProperty("driver", "org.postgresql.Driver")

  private val url: String = "jdbc:postgresql://localhost:5432/rtjvm"

  def getData(table: String): DataFrame = {
    sparkSession.read.jdbc(url, s"public.$table", properties)
  }

  val employeesDataFrame: DataFrame = getData("employees")
  val salariesDataFrame: DataFrame = getData("salaries")
  val managersDataFrame: DataFrame = getData("dept_manager")
  val titlesDataFrame: DataFrame = getData("titles")

  logger.debug("Show all employees and their max salary")
  val maxSalaries: DataFrame = salariesDataFrame.groupBy("emp_no").agg(max("salary").as("maxSalary"))
  maxSalaries.show

  logger.debug("Show all employees who were never managers")
  employeesDataFrame
    .join(managersDataFrame, employeesDataFrame.col("emp_no") === managersDataFrame.col("emp_no"), "left_anti").show

  logger.debug("Find the job titles of the ten best paid employees")
  val mostRecentTitlesDataFrame: DataFrame = titlesDataFrame.groupBy("emp_no", "title").agg(max("to_date"))
  val bestPaidEmployeesDataFrame: DataFrame = maxSalaries.orderBy(col("maxSalary").desc).limit(10)
  val bestPaidJobsDataFrame: DataFrame = bestPaidEmployeesDataFrame.join(mostRecentTitlesDataFrame, "emp_no")
  bestPaidJobsDataFrame.show()
}