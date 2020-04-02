package dataframes.columns.and.expressions.utils

import dataframes.TestUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.funsuite.AnyFunSuite

class DropColumnUtilsTest extends AnyFunSuite {

  test("DropColumnUtils.drop") {
    val sparkSession: SparkSession = TestUtils.sparkSession
    val dataFrame: DataFrame = sparkSession.read.json("src/test/resources/data/more_cars.json")
    val result: Array[String] = DropColumnUtils.drop(dataFrame, "Name").columns

    assert(result.contains("Name").equals(false))
    assert(result.contains("Miles_per_Gallon").equals(true))
    assert(result.contains("Cylinders").equals(true))
    assert(result.contains("Displacement").equals(true))
    assert(result.contains("Horsepower").equals(true))
    assert(result.contains("Weight_in_lbs").equals(true))
    assert(result.contains("Acceleration").equals(true))
    assert(result.contains("Year").equals(true))
    assert(result.contains("Origin").equals(true))
  }
}