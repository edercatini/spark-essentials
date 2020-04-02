package dataframes.columns.and.expressions.utils

import org.apache.spark.sql.DataFrame

object UniteDataFrameUtils {

  def union(rawDataFrame: DataFrame, anotherDataFrame: DataFrame): DataFrame = {
    rawDataFrame.union(anotherDataFrame)
  }
}