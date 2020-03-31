package dataframes.basics

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object ModuleExercises extends App {
  /**
   1. Create a DataFrame describing: smartphone and save it to a CSV file
    - make
    - model
    - processor
    - screen dimension
    - camera
   2. Read CSV file from /data and turn it into a DataFrame
    - define a custom schema and print it
    - count the number of rows
   */

  val sparkSession: SparkSession = SparkSession.builder
    .appName("moduleExercise")
    .config("spark.master", "local")
    .getOrCreate

  val fileSystem: FileSystem = FileSystem.get(new Configuration)

  val path: String = "src/main/resources/data/first-exercise"

  def firstExercise(): Unit = {
    import sparkSession.implicits._

    val cellphoneData = Seq(
      ("LG Electronics/Google", "Nexus 5X", "Exynos 990", "1080×1920", "16MP"),
      ("Nexus 6P", "Huawei/Google", "Snapdragon 865", "1080×1920", "12MP"),
      ("Nokia X", "Nokia", "MediaTek Dimensity 1000", "1080×1920", "8MP"),
      ("Apple iPhone 11", "Apple A13 Bionic", "Apple A13 Bionic", "1080×1920", "12MP")
    )

    val dataFrame: DataFrame = cellphoneData.toDF("Make", "Model", "Processor", "Screen Dimension", "Camera")

    dataFrame.write
      .mode(SaveMode.Overwrite)
      .csv(path)
  }

  def secondExercise(): Unit = {

    def setDataFrameSchema(): StructType = {
      StructType(
        Array(
          StructField("Make", StringType),
          StructField("Model", StringType),
          StructField("Processor", StringType),
          StructField("Screen Dimension", StringType),
          StructField("Camera", StringType)
        )
      )
    }

    def printDataFrameAndSchema(dataFrame: DataFrame): Unit = {
      dataFrame.show
      dataFrame.printSchema
      println(dataFrame.count)
    }

    def removeDataFrameFiles(): Unit = {
      fileSystem.delete(new Path(path), true)
    }

    val file: String = fileSystem.listStatus(new Path(path))
      .filter(fileStatus => fileStatus.getPath.getName.endsWith(".csv"))
      .map(fileStatus => fileStatus.getPath.toString)
      .head

    val dataFrame: DataFrame = sparkSession.read
      .schema(setDataFrameSchema())
      .csv(s"$file")

    printDataFrameAndSchema(dataFrame)

    removeDataFrameFiles()
  }

  firstExercise()

  secondExercise()
}