package dataframes.columns.and.expressions.utils

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, expr}

object AddColumnUtils {

  def addWithExpr(dataFrame: DataFrame): DataFrame = {
    dataFrame.withColumn("Total_Profit", expr("US_Gross + Worldwide_Gross"))
      .select(expr("Title"), expr("US_Gross"), expr("Worldwide_Gross"), expr("Total_Profit"))
  }

  def addWithCol(dataFrame: DataFrame): DataFrame = {
    dataFrame.withColumn("Total_Profit", col("US_Gross") + col("Worldwide_Gross"))
      .select(expr("Title"), expr("US_Gross"), expr("Worldwide_Gross"), expr("Total_Profit"))
  }
}