package dataframes.columns.and.expressions.utils

import org.apache.spark.sql.DataFrame

object DropColumnUtils {

  def drop(dataFrame: DataFrame, column: String): DataFrame = {
    dataFrame.drop(column)
  }
}