package com.github.spark.etl.core.app.utils

import scala.math.BigDecimal.RoundingMode

object MathUtils {
  def round(value: Double, scale: Int): BigDecimal =
    BigDecimal(value).setScale(scale, RoundingMode.HALF_UP)
  def round(value: BigDecimal, scale: Int): BigDecimal = value.setScale(scale, RoundingMode.HALF_UP)
}
