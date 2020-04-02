package dataframes.datasources.writer

import dataframes.TestUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.funsuite.AnyFunSuite

class ParquetDataFrameWriterTest extends AnyFunSuite {

  val outputPath: String = TestUtils.path
  val writer: ParquetDataFrameWriter = new ParquetDataFrameWriter
  val fileSystem: FileSystem = FileSystem.get(new Configuration)
  val sparkSession: SparkSession = TestUtils.sparkSession
  val dataFrame: DataFrame = sparkSession.read
    .json(s"$outputPath/movies.json")

  test("ParquetDataFrameWriter.mustCreateParquetFile") {
    writer.write(dataFrame, outputPath)

    val path: Path = new Path(s"$outputPath/parquet-output")

    val generatedFile: Array[FileStatus] = fileSystem.listStatus(path)
      .filter(fileStatus => fileStatus.getPath.getName.endsWith("snappy.parquet"))

    assert(fileSystem.isDirectory(path).equals(true))
    assert(generatedFile.length.equals(1))

    fileSystem.delete(path, true)
  }
}