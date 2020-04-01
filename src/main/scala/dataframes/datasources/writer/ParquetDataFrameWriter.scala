package dataframes.datasources.writer

import org.apache.spark.sql.{DataFrame, SaveMode}

class ParquetDataFrameWriter extends DataFrameWriter {

  override def write(dataFrame: DataFrame, outputPath: String): Unit = {
    dataFrame.write
      .mode(SaveMode.Overwrite)
      .save(s"$outputPath/parquet-output")
  }
}