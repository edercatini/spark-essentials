package dataframes.aggregations

import dataframes.TestUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.funsuite.AnyFunSuite

class DataAggregationLectureTest extends AnyFunSuite {

  private val sparkSession: SparkSession = TestUtils.sparkSession

  private val dataFrame: DataFrame = sparkSession.read
      .option("inferSchema", "true")
      .json("src/test/resources/data/movies.json")

  test("DataAggregationLecture.count") {
    val resultFunction: DataFrame = DataAggregationLecture.countFunction(dataFrame, "Major_Genre")
    val resultExpr: DataFrame = DataAggregationLecture.countExpr(dataFrame,"Major_Genre")

    assert(resultFunction.first.toString.equals("[2926]"))
    assert(resultExpr.first.toString.equals("[2926]"))
  }

  test("DataAggregationLecture.countDistinctData") {
    val result: DataFrame = DataAggregationLecture.countDistinctData(dataFrame,"Major_Genre")
    assert(result.first.toString.equals("[12]"))
  }

  test("DataAggregationLecture.groupByColumn") {
    val result: DataFrame = DataAggregationLecture.groupByColumn(dataFrame,"Major_Genre")
    assert(result.first.toString.equals("[Drama,789]"))
  }

  test("DataAggregationLecture.countEverything") {
    val count: DataFrame = DataAggregationLecture.countEverything(dataFrame)
    val countExpr: DataFrame = DataAggregationLecture.countEverythingExpr(dataFrame)

    assert(count.first.toString.equals("[3201]"))
    assert(countExpr.first.toString.equals("[3201]"))
  }
}