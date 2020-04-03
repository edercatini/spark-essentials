package dataframes.aggregations

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object DataAggregationLecture extends App {

  val sparkSession: SparkSession = SparkSession.builder
    .appName("app")
    .config("spark.master", "local")
    .getOrCreate

  val moviesDataFrame: DataFrame = sparkSession.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  def countFunction(moviesDataFrame: DataFrame, columnName: String): DataFrame = {
    moviesDataFrame.select(count(columnName))
  }

  def countExpr(moviesDataFrame: DataFrame, columnName: String): DataFrame = {
    moviesDataFrame.selectExpr(s"count($columnName)")
  }

  def countDistinctData(moviesDataFrame: DataFrame, columnName: String): DataFrame = {
    moviesDataFrame.select(countDistinct(columnName))
  }

  def groupByColumn(moviesDataFrame: DataFrame, columnName: String): DataFrame = {
    moviesDataFrame.groupBy(col(columnName)).count
  }

  def countEverything(moviesDataFrame: DataFrame): DataFrame = {
    moviesDataFrame.select(count("*"))
  }

  def countEverythingExpr(moviesDataFrame: DataFrame): DataFrame = {
    moviesDataFrame.selectExpr("count('*')")
  }

  countFunction(moviesDataFrame, "Major_Genre").show

  countExpr(moviesDataFrame, "Major_Genre").show

  countDistinctData(moviesDataFrame, "Major_Genre").show

  groupByColumn(moviesDataFrame, "Major_Genre").show

  countEverything(moviesDataFrame).show

  countEverythingExpr(moviesDataFrame).show
}