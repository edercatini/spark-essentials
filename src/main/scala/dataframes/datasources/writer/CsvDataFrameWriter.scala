package dataframes.datasources.writer

import org.apache.spark.sql.{DataFrame, SaveMode}

class CsvDataFrameWriter extends DataFrameWriter {

  override def write(dataFrame: DataFrame, outputPath: String): Unit = {
    dataFrame.write
      .mode(SaveMode.Overwrite)
      .options(Map(
        "sep" -> "\t",
        "dateFormat" -> "dd-MMM-yy"
      ))
      .csv(s"$outputPath/csv-output")
  }
}