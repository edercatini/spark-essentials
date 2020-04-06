package typesdatasets.types

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object ComplexTypeExercise extends App {

  val sparkSession: SparkSession = SparkSession.builder
    .appName("appName")
    .config("spark.master", "local")
    .getOrCreate

  val stocksDataFrame: DataFrame = sparkSession.read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("src/main/resources/data/stocks.csv")

  val formattedStocksDate: DataFrame = stocksDataFrame
    .withColumn("actual_date", to_date(col("date"), "MMM d yyyy"))

  formattedStocksDate.show
}