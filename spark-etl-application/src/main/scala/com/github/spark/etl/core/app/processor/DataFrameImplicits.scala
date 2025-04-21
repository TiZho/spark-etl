package com.github.spark.etl.core.app.processor

import org.apache.spark.sql
import org.apache.spark.sql.Column

import scala.collection.immutable.ListMap

object DataFrameImplicits {
  implicit class DataFrameExtended(df: sql.DataFrame) {
    def withPartitionColumns(newCol: ListMap[String, Column]): sql.DataFrame =
      df.select(newCol.map { case (colName, colValue) => colValue.as(colName) }.toList: _*)
  }
}
