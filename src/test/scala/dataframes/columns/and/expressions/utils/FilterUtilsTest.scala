package dataframes.columns.and.expressions.utils

import dataframes.TestUtils
import dataframes.columns.and.expressions.utils.FilterUtils.{conditionFilter, stringFilter}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.Assertion
import org.scalatest.funsuite.AnyFunSuite

class FilterUtilsTest extends AnyFunSuite {

  private val sparkSession: SparkSession = TestUtils.sparkSession

  private val path: String = TestUtils.path

  private val dataFrame: DataFrame = sparkSession.read.json(s"$path/movies.json")

  private def assertClause(dataFrame: DataFrame): Assertion = {
    assert(dataFrame.count() == 1)
  }

  test("FilterUtils.stringFilter") {
    val result: DataFrame = stringFilter(dataFrame)
      .select("Major_Genre")
      .distinct

    assertClause(result)
  }

  test("FilterUtils.conditionFilter") {
    val result: DataFrame = conditionFilter(dataFrame)
      .select("Major_Genre")
      .distinct

    assertClause(result)
  }
}