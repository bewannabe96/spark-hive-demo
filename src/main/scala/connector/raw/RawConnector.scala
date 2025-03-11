package com.bewannabe.idus
package connector.raw

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.nio.file.Paths

private object RawConnector {
  private val BASE_PATH: String = "spark-warehouse/raw"
}

abstract class RawConnector(val schema: StructType) extends Serializable {
  def extract(sparkSession: SparkSession, paths: Seq[String]): DataFrame = {
    sparkSession
      .read
      .option("header", "true")
      .schema(schema)
      .csv(paths.map(v => Paths.get(RawConnector.BASE_PATH, v).toString): _*)
  }
}

