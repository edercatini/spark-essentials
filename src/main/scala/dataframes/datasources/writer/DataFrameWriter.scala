package dataframes.datasources.writer

import org.apache.spark.sql.DataFrame

trait DataFrameWriter {

  def write(dataFrame: DataFrame, outputPath: String)
}