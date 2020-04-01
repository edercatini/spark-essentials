package dataframes

import org.apache.spark.sql.SparkSession

object TestUtils {

  val outputPath: String = "src/test/resources/data"

  val sparkSession: SparkSession = SparkSession.builder
    .appName("appTest")
    .config("spark.master", "local")
    .getOrCreate
}