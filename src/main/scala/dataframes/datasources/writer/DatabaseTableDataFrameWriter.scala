package dataframes.datasources.writer

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SaveMode}

class DatabaseTableDataFrameWriter extends DataFrameWriter {

  override def write(dataFrame: DataFrame, outputPath: String): Unit = {

    def getConnectionProperties(): Properties = {
      val connectionProperties: Properties = new Properties
      connectionProperties.setProperty("driver", "org.postgresql.Driver")
      connectionProperties.setProperty("user", "docker")
      connectionProperties.setProperty("password", "docker")

      connectionProperties
    }

    dataFrame.write
      .mode(SaveMode.Overwrite)
      .jdbc("jdbc:postgresql://localhost:5432/rtjvm", "public.movies", getConnectionProperties())
  }
}