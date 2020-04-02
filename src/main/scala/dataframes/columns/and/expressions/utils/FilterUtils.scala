package dataframes.columns.and.expressions.utils

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

object FilterUtils {

  def stringFilter(dataFrame: DataFrame): DataFrame = {
    dataFrame.filter("Major_Genre = 'Comedy' and IMDB_Rating > '6'")
  }

  def conditionFilter(dataFrame: DataFrame): DataFrame = {
    dataFrame.filter(col("Major_Genre") === "Comedy" and col("IMDB_Rating") > 6)
  }
}