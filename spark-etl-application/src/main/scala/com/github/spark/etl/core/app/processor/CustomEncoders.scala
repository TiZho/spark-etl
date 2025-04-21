package com.github.spark.etl.core.app.processor

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

import scala.reflect.runtime.universe._

object CustomEncoders {
  implicit def customEncoder[I: TypeTag]: Encoder[I] = ExpressionEncoder[I]()
}
