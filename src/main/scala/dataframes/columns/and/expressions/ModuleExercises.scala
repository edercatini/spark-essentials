package dataframes.columns.and.expressions

import dataframes.columns.and.expressions.utils.AddColumnUtils._
import dataframes.columns.and.expressions.utils.DropColumnUtils.drop
import dataframes.columns.and.expressions.utils.FilterUtils.{conditionFilter, stringFilter}
import dataframes.columns.and.expressions.utils.SelectUtils._
import dataframes.columns.and.expressions.utils.UniteDataFrameUtils.union
import org.apache.spark.sql.{DataFrame, SparkSession}

object ModuleExercises extends App {

  val path: String = "src/main/resources/data"

  val sparkSession: SparkSession = SparkSession.builder
    .appName("appName")
    .config("spark.master", "local")
    .getOrCreate

  val moviesDataFrame: DataFrame = sparkSession.read
    .option("inferSchema", "true")
    .json(s"$path/movies.json")

  def showDataFrame(dataFrames: Seq[DataFrame]): Unit = {
    dataFrames.foreach(dataFrame => dataFrame.show)
  }

  val twoColumnDataFrameString: DataFrame = selectByTwoStringColumns(moviesDataFrame, "Title", "Production_Budget")
  val twoColumnDataFrameColMethod: DataFrame = selectByDataFrameColMethod(moviesDataFrame, "Title", "Production_Budget")
  val twoColumnDataFrameColFunction: DataFrame = selectByColFunction(moviesDataFrame, "Title", "Production_Budget")
  val twoColumnDataFrameColExpr: DataFrame = selectByExpr(moviesDataFrame, "Title", "Production_Budget")
  val dataFrameWithAdditionalColumnWithExpr: DataFrame = addWithExpr(moviesDataFrame)
  val dataFrameWithAdditionalColumnWithCol: DataFrame = addWithCol(moviesDataFrame)
  val dataFrameAfterColumnDrop: DataFrame = drop(dataFrameWithAdditionalColumnWithCol, "Total_Profit")
  val filteredDataFrameString: DataFrame = stringFilter(moviesDataFrame)
  val filteredDataFrameCondition: DataFrame = conditionFilter(moviesDataFrame)

  showDataFrame(Seq(
    twoColumnDataFrameString,
    twoColumnDataFrameColMethod,
    twoColumnDataFrameColFunction,
    twoColumnDataFrameColExpr,
    dataFrameWithAdditionalColumnWithExpr,
    dataFrameWithAdditionalColumnWithCol,
    dataFrameAfterColumnDrop,
    filteredDataFrameString,
    filteredDataFrameCondition
  ))

  val carsDataFrame: DataFrame = sparkSession.read.json(s"$path/cars.json")
  val moreCarsDataFrame: DataFrame = sparkSession.read.json(s"$path/more_cars.json")
  val unionDataFrame: DataFrame = union(carsDataFrame, moreCarsDataFrame)
  println(unionDataFrame.count().equals(carsDataFrame.count + moreCarsDataFrame.count))
}