package typesdatasets.types

import org.apache.spark.sql.functions.{col, regexp_extract}
import org.apache.spark.sql.{DataFrame, SparkSession}

object CommonTypeExercise extends App {

  val sparkSession: SparkSession = SparkSession.builder
    .appName("appName")
    .config("spark.master", "local")
    .getOrCreate

  def regexExercise(): Unit = {
    def getCarBrandsMockAPI: List[String] = List("volkswagen", "vw", "mercedes", "ford")

    val carsDataFrame: DataFrame = sparkSession.read
      .option("inferSchema", "true")
      .option("header", "true")
      .json("src/main/resources/data/cars.json")

    val regex: String = getCarBrandsMockAPI.mkString("|")

    val specificCarsDataFrame: DataFrame = carsDataFrame
      .select(col("Name"), regexp_extract(col("Name"), regex, 0).as("regex"))
      .where(col("regex") notEqual "")
      .orderBy(col("regex").desc)

    specificCarsDataFrame.show(200)
  }

  regexExercise()
}
