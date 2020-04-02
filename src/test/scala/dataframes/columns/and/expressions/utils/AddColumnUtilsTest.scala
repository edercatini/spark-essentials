package dataframes.columns.and.expressions.utils

import dataframes.TestUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.Assertion
import org.scalatest.funsuite.AnyFunSuite

class AddColumnUtilsTest extends AnyFunSuite {

  private val path: String = TestUtils.path

  private val sparkSession: SparkSession = TestUtils.sparkSession

  private val dataFrame: DataFrame =  sparkSession.read.json(s"$path/movies.json")

  private def assertClause(columns: Array[String]): Assertion = {
    assert(columns.length.equals(4))
    assert(columns(0).equals("Title"))
    assert(columns(1).equals("US_Gross"))
    assert(columns(2).equals("Worldwide_Gross"))
    assert(columns(3).equals("Total_Profit"))
  }

  test("AddColumnUtils.addWithExpr") {
    val result: Array[String] = AddColumnUtils.addWithExpr(dataFrame).columns
    assertClause(result)
  }

  test("AddColumnUtils.addWithCol") {
    val result: Array[String] = AddColumnUtils.addWithExpr(dataFrame).columns
    assertClause(result)
  }
}