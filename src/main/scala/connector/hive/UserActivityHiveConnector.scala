package com.bewannabe.idus
package connector.hive

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, YearMonth}

object UserActivityHiveConnector extends HiveConnector(
  tableName = "user_activity",
  schema = new StructType()
    .add("event_date_kst", DateType, nullable = false)
    .add("event_ts_utc", TimestampType, nullable = false)
    .add("event_type", StringType, nullable = false)
    .add("session_id", StringType, nullable = false)
    .add("user_id", StringType, nullable = false)
    .add("price", IntegerType, nullable = true)
    .add("product_id", StringType, nullable = true)
    .add("brand", StringType, nullable = true)
    .add("category_id", StringType, nullable = true)
    .add("category_code", StringType, nullable = true),
  partitionKeys = Seq("event_date_kst")
) {
  def load(dataFrame: DataFrame, yearMonthStrings: Seq[String]): Unit = {
    var df = dataFrame.select(orderedColumns: _*)

    def appendPartition(firstDate: LocalDate, lastDate: LocalDate): Unit = {
      df = df.union(this.extract(
        dataFrame.sparkSession,
        s"""
           |SELECT *
           |FROM {TABLE}
           |WHERE
           |(event_date_kst = '${firstDate.toString}'
           |  AND event_ts_utc < '${firstDate.toString} 00:00:00')
           |OR
           |(event_date_kst = '${lastDate.toString}'
           |  AND event_ts_utc >= '${lastDate.toString} 00:00:00')
           |""".stripMargin
      ))
    }

    val formatter = DateTimeFormatter.ofPattern("yyyy-MM")

    val yearMonths = yearMonthStrings.map(value => YearMonth.parse(value, formatter))
    var firstYearMonth = yearMonths.head
    var lastYearMonth = firstYearMonth
    for (i <- 1 until yearMonths.length) {
      val current = yearMonths(i)
      if (current == lastYearMonth.plusMonths(1)) {
        lastYearMonth = current
      } else {
        appendPartition(firstYearMonth.atDay(1), lastYearMonth.atEndOfMonth().plusDays(1))
        firstYearMonth = current
        lastYearMonth = current
      }
    }
    appendPartition(firstYearMonth.atDay(1), lastYearMonth.atEndOfMonth().plusDays(1))

    super.load(df)
  }
}
