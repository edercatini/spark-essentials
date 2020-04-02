package dataframes.columns.and.expressions.utils

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, expr}

object SelectUtils {

  def selectByTwoStringColumns(dataFrame: DataFrame, firstColumn: String, secondColumn: String): DataFrame = {
    dataFrame.select(firstColumn, secondColumn)
  }

  def selectByDataFrameColMethod(dataFrame: DataFrame, firstColumn: String, secondColumn: String): DataFrame = {
    dataFrame.select(dataFrame.col(firstColumn), dataFrame.col(secondColumn))
  }

  def selectByColFunction(dataFrame: DataFrame, firstColumn: String, secondColumn: String): DataFrame = {
    dataFrame.select(col(firstColumn), col(secondColumn))
  }

  def selectByExpr(dataFrame: DataFrame, firstColumn: String, secondColumn: String): DataFrame = {
    dataFrame.select(expr(firstColumn), expr(secondColumn))
  }
}