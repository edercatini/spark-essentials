package dataframes.columns.and.expressions.utils

import dataframes.TestUtils
import dataframes.columns.and.expressions.utils.SelectUtils._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.Assertion
import org.scalatest.funsuite.AnyFunSuite

class SelectUtilsTest extends AnyFunSuite {

  private val sparkSession: SparkSession = TestUtils.sparkSession

  private val path: String = TestUtils.path

  private val dataFrame: DataFrame = sparkSession.read.json(s"$path/movies.json")

  private def assertClause(columns: Array[String]): Assertion = {
    assert(columns.length.equals(2))
    assert(columns(0).equals("Title"))
    assert(columns(1).equals("Major_Genre"))
  }

  test("SelectUtils.selectByTwoStringColumns") {
    val result: Array[String] = selectByTwoStringColumns(dataFrame, "Title", "Major_Genre").columns
    assertClause(result)
  }

  test("SelectUtils.selectByDataFrameColMethod") {
    val result: Array[String] = selectByDataFrameColMethod(dataFrame, "Title", "Major_Genre").columns
    assertClause(result)
  }

  test("SelectUtils.selectByColFunction") {
    val result: Array[String] = selectByColFunction(dataFrame, "Title", "Major_Genre").columns
    assertClause(result)
  }

  test("SelectUtils.selectByExpr") {
    val result: Array[String] = selectByExpr(dataFrame, "Title", "Major_Genre").columns
    assertClause(result)
  }
}