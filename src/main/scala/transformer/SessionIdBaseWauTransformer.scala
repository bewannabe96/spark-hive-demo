package com.bewannabe.idus
package transformer

import connector.hive.UserActivityHiveConnector

import org.apache.spark.sql.SparkSession

object SessionIdBaseWauTransformer {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession
      .builder
      .appName(UserIdBaseWauTransformer.getClass.getName)
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    val startDateString = args(0)
    val endDateString = args(1)

    UserActivityHiveConnector.extract(
      sparkSession,
      s"""
         |WITH user_activity_with_week AS (
         |SELECT
         |  session_id,
         |  DATE_TRUNC('WEEK', event_date_kst) AS event_week
         |FROM {TABLE}
         |)
         |
         |SELECT
         |  event_week,
         |  COUNT(DISTINCT session_id) AS wau
         |FROM user_activity_with_week
         |WHERE
         |  event_week >= DATE_TRUNC('WEEK', '$startDateString')
         |  AND event_week <= DATE_TRUNC('WEEK', '$endDateString')
         |GROUP BY event_week
         |ORDER BY event_week ASC;
         |""".stripMargin
    ).show()
  }
}
