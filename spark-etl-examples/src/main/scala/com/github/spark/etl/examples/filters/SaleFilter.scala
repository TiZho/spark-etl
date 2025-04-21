package com.github.spark.etl.examples.filters

import com.github.spark.etl.core.app.config.Env
import org.apache.spark.sql.functions._
import com.github.spark.etl.core.app.processor.Filter
import com.github.spark.etl.examples.config.SalesConfig
import com.github.spark.etl.examples.domain.Sale
import com.github.spark.etl.examples.utils.TimeUtils.firstDayOfMonth
import org.apache.spark.sql.{Column, SparkSession}

class SaleFilter extends Filter[Sale, SalesConfig]{
  override def filter(implicit config: SalesConfig,
                      env: Env,
                      spark: SparkSession): Sale => Boolean =
    sale => sale.timestamp.after(firstDayOfMonth)

  override def filterColumn(implicit config: SalesConfig,
                            env: Env,
                            spark: SparkSession): Column =
    col(Sale.timestampCol) > lit(firstDayOfMonth)
}
