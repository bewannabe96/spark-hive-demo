package com.bewannabe.idus
package connector.hive

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}

abstract class HiveConnector(
                              tableName: String,
                              schema: StructType,
                              partitionKeys: Seq[String],
                            ) {
  protected val orderedColumns: Seq[Column] =
    (schema.fieldNames.filter(field => !partitionKeys.contains(field)) ++ partitionKeys)
      .map(field => col(field))

  private def createTable(sparkSession: SparkSession): Unit = {
    sparkSession
      .createDataFrame(sparkSession.sparkContext.emptyRDD[Row], schema)
      .write
      .mode("ignore")
      .format("parquet")
      .option("compression", "snappy")
      .option("tableType", "EXTERNAL")
      .partitionBy(partitionKeys: _*)
      .saveAsTable(tableName)
  }

  def extract(sparkSession: SparkSession, query: String): DataFrame = {
    this.createTable(sparkSession)
    sparkSession.sql(s"${query.replace("{TABLE}", tableName)}")
  }

  protected def load(dataFrame: DataFrame): Unit = {
    this.createTable(dataFrame.sparkSession)

    val tempTableName = s"temp_${tableName}_${System.currentTimeMillis()}"

    dataFrame
      .select(orderedColumns: _*)
      .write
      .format("parquet")
      .option("compression", "snappy")
      .option("tableType", "EXTERNAL")
      .partitionBy(partitionKeys: _*)
      .saveAsTable(tempTableName)

    dataFrame.sparkSession.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    dataFrame.sparkSession.sql(
      s"""
         |INSERT OVERWRITE TABLE $tableName
         |PARTITION(${partitionKeys.mkString(", ")})
         |SELECT * FROM $tempTableName
         |""".stripMargin)

    dataFrame.sparkSession.sql(s"DROP TABLE IF EXISTS $tempTableName")
  }
}

