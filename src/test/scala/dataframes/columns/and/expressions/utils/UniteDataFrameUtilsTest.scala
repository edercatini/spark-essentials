package dataframes.columns.and.expressions.utils

import dataframes.TestUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.funsuite.AnyFunSuite

class UniteDataFrameUtilsTest extends AnyFunSuite {

  test("UniteDataFrameUtils.unite") {
    val sparkSession: SparkSession = TestUtils.sparkSession
    val path: String = TestUtils.path
    val firstDataFrame: DataFrame = sparkSession.read.json(s"$path/cars.json")
    val secondDataFrame: DataFrame = sparkSession.read.json(s"$path/more_cars.json")
    val result: DataFrame = UniteDataFrameUtils.union(firstDataFrame, secondDataFrame)
    assert(result.count.equals(firstDataFrame.count + secondDataFrame.count))
  }
}