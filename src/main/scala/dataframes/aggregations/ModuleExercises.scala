package dataframes.aggregations

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object ModuleExercises extends App {

  val sparkSession: SparkSession = SparkSession.builder
    .appName("AppName")
    .config("spark.master", "local")
    .getOrCreate

  val moviesDataFrame: DataFrame = sparkSession.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")


  val profitSum: DataFrame = moviesDataFrame
    .select((col("US_Gross") + col("Worldwide_Gross") + col("US_DVD_Sales")).as("Total_Profit"))
    .select(sum("Total_Profit"))

  profitSum.show

  val distinctDirectors: DataFrame = moviesDataFrame
    .select(countDistinct("Director")).as("Directors")

  distinctDirectors.show

  val meanAndStandardDeviationProfit: DataFrame = moviesDataFrame
    .select((col("US_Gross") + col("Worldwide_Gross") + col("US_DVD_Sales")).as("Total_Profit"))
    .select(mean("Total_Profit").as("Mean"), stddev("Total_Profit").as("Standard_Deviation"))

  meanAndStandardDeviationProfit.show

  val averageData: DataFrame = moviesDataFrame
    .groupBy("Director")
    .agg(
      avg("IMDB_Rating").as("IMDB_Rating_AVG"),
      avg(col("US_Gross") + col("Worldwide_Gross") + col("US_DVD_Sales")).as("Total_Profit_AVG")
    )
    .orderBy(col("IMDB_Rating_AVG").desc_nulls_last)

  averageData.show
}