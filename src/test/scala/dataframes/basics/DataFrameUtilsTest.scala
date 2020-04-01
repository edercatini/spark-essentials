package dataframes.basics

import dataframes.TestUtils
import dataframes.basics.DataFrameUtils._
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.Assertion
import org.scalatest.funsuite.AnyFunSuite
import org.slf4j.{Logger, LoggerFactory}

class DataFrameUtilsTest extends AnyFunSuite {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)
  private val sparkSession: SparkSession = TestUtils.sparkSession
  private val jsonPath: String = s"${TestUtils.outputPath}/cars.json"

  test("DataFrameUtils.MustReadADataFrameWithInferSchema") {
    val dataFrame: DataFrame = readDataFrameWithInferSchema(sparkSession, jsonPath)
    val analyzedRow: Row = dataFrame.first

    printAndShowDataFrameSchema(dataFrame)

    assert(analyzedRow.get(0).equals(12.0))
    assert(analyzedRow.get(1).equals(8L))
    assert(analyzedRow.get(2).equals(307.0))
    assert(analyzedRow.get(3).equals(130L))
    assert(analyzedRow.get(4).equals(18.0))
    assert(analyzedRow.get(5).equals("chevrolet chevelle malibu"))
    assert(analyzedRow.get(6).equals("USA"))
    assert(analyzedRow.get(7).equals(3504L))
    assert(analyzedRow.get(8).equals("1970-01-01"))
  }

  test("DataFrameUtils.MustReadADataFrameWithCustomSchema") {
    val customSchema: StructType = retrieveCarsCustomDataFrameSchema()
    val dataFrame: DataFrame = readDataFrameWithCustomSchema(sparkSession, jsonPath, customSchema)
    val analyzedRow: Row = dataFrame.first

    assert(analyzedRow.get(0).equals("chevrolet chevelle malibu"))
    assert(analyzedRow.get(1).equals(18))
    assert(analyzedRow.get(2).equals(8))
    assert(analyzedRow.get(3).equals(307))
    assert(analyzedRow.get(4).equals(130))
    assert(analyzedRow.get(5).equals(3504))
    assert(analyzedRow.get(6).equals(12.0))
    assert(analyzedRow.get(7).toString.equals("1970-01-01"))
    assert(analyzedRow.get(8).equals("USA"))
  }

  test("DataFrameUtils.MustRetrieveProperCustomSchema") {

    def assertData(analyzedField: StructField, expectedName: String, expectedDataType: String): Assertion = {
      logger.debug("expectedName={}", expectedName)
      assert(analyzedField.name.equals(expectedName))
      assert(analyzedField.dataType.toString.equals(expectedDataType))
      assert(analyzedField.nullable.equals(true))
    }

    val schema: Array[StructField] = retrieveCarsCustomDataFrameSchema().toArray

    assertData(schema.apply(0), "Name", "StringType")
    assertData(schema.apply(1), "Miles_per_Gallon", "IntegerType")
    assertData(schema.apply(2), "Cylinders", "IntegerType")
    assertData(schema.apply(3), "Displacement", "IntegerType")
    assertData(schema.apply(4), "Horsepower", "IntegerType")
    assertData(schema.apply(5), "Weight_in_lbs", "IntegerType")
    assertData(schema.apply(6), "Acceleration", "DoubleType")
    assertData(schema.apply(7), "Year", "DateType")
    assertData(schema.apply(8), "Origin", "StringType")
  }

  test("DataFrameUtils.MustCreateCarsDataFrame") {
    val cars = Seq(
      ("Test", 1.5, 1L, 1.0, 1L, 1L, 1.0, "Test", "Test"),
      ("Test2", 2.5, 2L, 2.0, 2L, 2L, 2.0, "Test2", "Test2")
    )

    val dataFrame: DataFrame = createCarsDataFrame(sparkSession, cars)
    printAndShowDataFrameSchema(dataFrame)

    val firstRow: Row = dataFrame.first
    assert(firstRow.get(0).equals("Test"))
    assert(firstRow.get(1).equals(1.5))
    assert(firstRow.get(2).equals(1L))
    assert(firstRow.get(3).equals(1.0))
    assert(firstRow.get(4).equals(1L))
    assert(firstRow.get(5).equals(1L))
    assert(firstRow.get(6).equals(1.0))
    assert(firstRow.get(7).equals("Test"))
    assert(firstRow.get(8).equals("Test"))

    val secondRow: Row = dataFrame.take(2)(1)
    assert(secondRow.get(0).equals("Test2"))
    assert(secondRow.get(1).equals(2.5))
    assert(secondRow.get(2).equals(2L))
    assert(secondRow.get(3).equals(2.0))
    assert(secondRow.get(4).equals(2L))
    assert(secondRow.get(5).equals(2L))
    assert(secondRow.get(6).equals(2.0))
    assert(secondRow.get(7).equals("Test2"))
    assert(secondRow.get(8).equals("Test2"))
  }
}