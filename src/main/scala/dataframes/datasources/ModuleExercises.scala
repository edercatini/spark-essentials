package dataframes.datasources

import dataframes.datasources.writer.{CsvDataFrameWriter, DatabaseTableDataFrameWriter, ParquetDataFrameWriter}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

object ModuleExercises extends App {

  private val path: String = "src/main/resources/data"
  private val csvWriter: CsvDataFrameWriter = new CsvDataFrameWriter
  private val parquetWriter: ParquetDataFrameWriter = new ParquetDataFrameWriter
  private val databaseTableDataFrameWriter: DatabaseTableDataFrameWriter = new DatabaseTableDataFrameWriter

  private val sparkSession: SparkSession = SparkSession.builder
    .appName("appName")
    .config("spark.master", "local")
    .getOrCreate

  def retrieveMoviesSchema(): StructType = {
    StructType(Array(
      StructField("Title", StringType),
      StructField("US_Gross", LongType),
      StructField("Worldwide_Gross", LongType),
      StructField("US_DVD_Sales", IntegerType),
      StructField("Production_Budget", LongType),
      StructField("Release_Date", DateType),
      StructField("MPAA_Rating", StringType),
      StructField("Running_Time_min", StringType),
      StructField("Distributor", StringType),
      StructField("Source", StringType),
      StructField("Major_Genre", StringType),
      StructField("Creative_Type", StringType),
      StructField("Director", StringType),
      StructField("Rotten_Tomatoes_Rating", DoubleType),
      StructField("IMDB_Rating", DoubleType),
      StructField("IMDB_Votes", IntegerType)
    ))
  }

  val moviesDataFrame: DataFrame = sparkSession.read
    .schema(retrieveMoviesSchema())
    .json("src/main/resources/data/movies.json")

  csvWriter.write(moviesDataFrame, path)

  parquetWriter.write(moviesDataFrame, path)

  databaseTableDataFrameWriter.write(moviesDataFrame, null)
}