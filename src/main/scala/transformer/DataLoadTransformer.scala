package com.bewannabe.idus
package transformer


import connector.hive.UserActivityHiveConnector
import connector.raw.RawUserEventConnector

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.YearMonth
import java.time.format.DateTimeFormatter
import java.util.UUID
import scala.collection.mutable.ListBuffer


object DataLoadTransformer {
  /**
   * @param args : List of yyyy-MM formatted strings of input months
   */
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession
      .builder
      .appName(DataLoadTransformer.getClass.getName)
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    import sparkSession.implicits._

    // # Extract
    // ====================================================
    var columns = Set(RawUserEventConnector.schema.fieldNames.map(name => col(name)): _*)
    val raw_data_df = RawUserEventConnector
      .extractWithYearMonthStrings(sparkSession, args: _*)
    // ====================================================

    // # Transform
    // ====================================================
    // ## Select used columns only
    columns -= $"user_session"
    var transformed_df = raw_data_df.select(columns.toSeq: _*)

    // ## Calculate `event_date_kst` from `event_time`
    transformed_df = transformed_df
      .withColumn("event_ts_utc", to_timestamp($"event_time", "yyyy-MM-dd HH:mm:ss 'UTC'"))
      .withColumn("event_ts_kst", from_utc_timestamp($"event_ts_utc", "Asia/Seoul"))
      .withColumn("event_date_kst", to_date($"event_ts_kst"))

    // ## Select used columns only
    columns += $"event_date_kst"
    columns += $"event_ts_utc"
    columns -= $"event_time"
    transformed_df = transformed_df.select(columns.toSeq: _*)

    // ## Re-assign `session_id` based on the following rule:
    // 동일 `user_id` 내에서 `event_time` 간격이 5분 이상인 경우
    // 세션 종료로 간주하고 새로운 세션 ID를 생성
    val generateUuid = udf(() => UUID.randomUUID().toString)
    transformed_df = transformed_df
      .withColumn(
        "prev_event_ts_utc",
        lag($"event_ts_utc", 1)
          .over(Window.partitionBy($"user_id").orderBy($"event_ts_utc"))
      )
      .withColumn(
        "is_new_session",
        when($"prev_event_ts_utc".isNull, lit(true))
          .otherwise(unix_timestamp($"event_ts_utc") - unix_timestamp($"prev_event_ts_utc") >= 300)
      )
      .withColumn(
        "session_id",
        when($"is_new_session", generateUuid()).otherwise(lit(null))
      )
      .replaceWithExistingSessionId(args)
      .withColumn(
        "session_id",
        last(col("session_id"), ignoreNulls = true)
          .over(Window.partitionBy($"user_id").orderBy($"event_ts_utc").rowsBetween(Window.unboundedPreceding, 0))
      )

    // ## Select used columns only
    columns += $"session_id"
    transformed_df = transformed_df.select(columns.toSeq: _*)
    // ====================================================

    // # Load
    // ====================================================
    UserActivityHiveConnector.load(transformed_df, args)
    // ====================================================
  }

  implicit class implicits(dataFrame: DataFrame) {
    def replaceWithExistingSessionId(yearMonthStrings: Array[String]): DataFrame = {
      import dataFrame.sparkSession.implicits._

      // ## Extract existing data prior to incoming data
      val parser = DateTimeFormatter.ofPattern("yyyy-MM")

      val existing_data_dfs = new ListBuffer[DataFrame]()
      for (yearMonthString <- yearMonthStrings) {
        val yearMonth = YearMonth.parse(yearMonthString, parser)
        val prevYearMonth = yearMonth.minusMonths(1)

        // only the months that are not going to be overwritten
        if (!yearMonthStrings.contains(prevYearMonth.toString)) {
          val partitionDate = yearMonth.atDay(1)
          val startDateTime = partitionDate.atStartOfDay()

          existing_data_dfs += UserActivityHiveConnector
            .extract(
              dataFrame.sparkSession,
              s"""
                 |SELECT * FROM {TABLE}
                 |WHERE
                 |event_date_kst = '${partitionDate.toString}'
                 |  AND event_ts_utc < '${startDateTime.toString}'
                 |  AND event_ts_utc >= '${startDateTime.minusMinutes(5).toString}';
                 |""".stripMargin
            )
            .withColumn(
              "last_event_ts_utc",
              max($"event_ts_utc").over(Window.partitionBy($"user_id"))
            )
            .filter($"event_ts_utc" === $"last_event_ts_utc")
            .select(
              $"user_id",
              $"session_id".as("existing_session_id"),
              $"event_ts_utc".as("last_event_ts_utc"),
            )
        }
      }

      val existing_data_df = existing_data_dfs.reduce(_.union(_))

      // ## Fill session ID if already exist
      dataFrame
        .join(existing_data_df, Seq("user_id"), "left")
        .withColumn(
          "is_continuous_session",
          $"prev_event_ts_utc".isNull &&
            unix_timestamp($"event_ts_utc") - unix_timestamp($"last_event_ts_utc") < 300
        )
        .withColumn(
          "prev_event_ts_utc",
          when($"is_continuous_session", $"last_event_ts_utc").otherwise($"prev_event_ts_utc")
        )
        .withColumn(
          "is_new_session",
          when($"is_continuous_session", lit(false)).otherwise($"is_new_session")
        )
        .withColumn(
          "session_id",
          when($"is_continuous_session", $"existing_session_id").otherwise($"session_id")
        )
        .drop("existing_session_id", "last_event_ts_utc", "is_continuous_session")
    }
  }
}
