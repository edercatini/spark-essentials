package typesdatasets.datasets

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object Datasets extends App {

  val sparkSession: SparkSession = SparkSession.builder
    .appName("appName")
    .config("spark.master", "local")
    .getOrCreate

  def loadDataFrame(fileName: String): DataFrame = {
    sparkSession.read
      .option("inferSchema", "true")
      .json(s"src/main/resources/data/$fileName")
  }

  import sparkSession.implicits._
  val carsDataFrame: DataFrame = loadDataFrame("cars.json")
  val carsDataset: Dataset[Car] = carsDataFrame.as[Car]

  val carsCount: Long = carsDataset.count
  println(carsDataset.count)

  val mostPowerfulCarsDataset: Dataset[Car] = carsDataset.filter(_.Horsepower.getOrElse(0L) > 140)
  println(mostPowerfulCarsDataset.count)

  val averageHorsePower: Long = carsDataset.map(_.Horsepower.getOrElse(0L)).reduce(_ + _) / carsCount
  println(averageHorsePower)
}