package dataframes.datasources.writer

import dataframes.TestUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.funsuite.AnyFunSuite

class CsvDataFrameWriterTest extends AnyFunSuite {

  val outputPath: String = TestUtils.path
  val writer: CsvDataFrameWriter = new CsvDataFrameWriter
  val fileSystem: FileSystem = FileSystem.get(new Configuration)
  val sparkSession: SparkSession = TestUtils.sparkSession
  val dataFrame: DataFrame = sparkSession.read
    .json(s"$outputPath/movies.json")

  test("CsvDataFrameWriter.mustCreateCsvFile") {
    writer.write(dataFrame, outputPath)

    val path: Path = new Path(s"$outputPath/csv-output")

    val generatedFile: Array[FileStatus] = fileSystem.listStatus(path)
      .filter(fileStatus => fileStatus.getPath.getName.endsWith("csv"))

    assert(fileSystem.isDirectory(path).equals(true))
    assert(generatedFile.length.equals(1))

    fileSystem.delete(path, true)
  }
}