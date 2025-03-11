package com.bewannabe.idus
package connector.raw

import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.YearMonth
import java.time.format.DateTimeFormatter
import java.util.Locale

object RawUserEventConnector extends RawConnector(
  new StructType()
    .add("event_time", StringType, nullable = false)
    .add("event_type", StringType, nullable = false)
    .add("product_id", StringType, nullable = false)
    .add("category_id", StringType, nullable = false)
    .add("category_code", StringType, nullable = true)
    .add("brand", StringType, nullable = true)
    .add("price", IntegerType, nullable = false)
    .add("user_id", StringType, nullable = false)
    .add("user_session", StringType, nullable = true)
) {
  private def extractWithYearMonths(sparkSession: SparkSession, yearMonths: YearMonth*): DataFrame = {
    val formatter = DateTimeFormatter.ofPattern("yyyy-LLL", Locale.ENGLISH)
    val yearMonthStrings = yearMonths.map(_.format(formatter) + ".csv")
    this.extract(sparkSession, yearMonthStrings)
  }

  def extractWithYearMonthStrings(sparkSession: SparkSession, yearMonthStrings: String*): DataFrame = {
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM")
    val yearMonths = yearMonthStrings.map(YearMonth.parse(_, formatter))
    this.extractWithYearMonths(sparkSession, yearMonths: _*)
  }
}
