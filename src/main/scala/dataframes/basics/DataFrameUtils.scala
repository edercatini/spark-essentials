package dataframes.basics

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

object DataFrameUtils {

  def readDataFrameWithInferSchema(sparkSession: SparkSession, jsonPath: String): DataFrame = {
    sparkSession.read
      .option("inferSchema", "true")
      .json(jsonPath)
  }

  def readDataFrameWithCustomSchema(sparkSession: SparkSession, jsonPath: String, schema: StructType): DataFrame = {
    sparkSession.read
      .schema(schema)
      .json(jsonPath)
  }

  def printAndShowDataFrameSchema(dataFrame: DataFrame): Unit = {
    dataFrame.show
    dataFrame.printSchema
  }

  def retrieveCarsCustomDataFrameSchema(): StructType = {
    StructType(
      Array(
        StructField("Name", StringType),
        StructField("Miles_per_Gallon", IntegerType),
        StructField("Cylinders", IntegerType),
        StructField("Displacement", IntegerType),
        StructField("Horsepower", IntegerType),
        StructField("Weight_in_lbs", IntegerType),
        StructField("Acceleration", DoubleType),
        StructField("Year", DateType),
        StructField("Origin", StringType)
      )
    )
  }

  def createCarsDataFrame(sparkSession: SparkSession, data: Seq[(String, Double, Long, Double, Long, Long, Double, String, String)]): DataFrame = {
    import sparkSession.implicits._
    data.toDF("Name", "MPG", "Cylinders", "Displacement", "HP", "Weight", "Acceleration", "Year", "CountryOrigin")
  }
}