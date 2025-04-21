package com.github.spark.etl.core.app.processor

import com.github.spark.etl.LensMacro
import Indicator.{IndicatorColumn, IndicatorField}
import com.github.spark.etl.core.app.utils.MathUtils
import monocle.Lens
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.round
import org.apache.spark.sql.types.DecimalType

trait Indicator[E, T] extends Serializable {
  def field: IndicatorField[E, T]
  @transient
  def column: IndicatorColumn
}
object Indicator {
  trait IndicatorField[E, T] extends Serializable {
    def compute(input: E): T
    def fieldSelector: Lens[E, T]
    def processCompute: E => E = input =>
      LensMacro.updateProperty[E, T](input, fieldSelector, compute(input))
  }

  trait IndicatorColumn {
    def computeColumn: IndicatorColumnMapping
  }

  final case class IndicatorColumnMapping(name: String, value: Column)

  abstract class RoundedIndicatorField[E] extends IndicatorField[E, BigDecimal] {
    def rawCompute(input: E): BigDecimal
    val scale: Int                             = 14
    override def compute(input: E): BigDecimal = MathUtils.round(rawCompute(input), scale)
  }
  abstract class OptionalRoundedIndicatorField[E] extends IndicatorField[E, Option[BigDecimal]] {
    def rawCompute(input: E): Option[BigDecimal]
    val scale: Int = 14
    override def compute(input: E): Option[BigDecimal] =
      rawCompute(input).map(MathUtils.round(_, scale))
  }

  abstract class RoundedIndicatorColumn extends IndicatorColumn {
    def rawComputeColumn: IndicatorColumnMapping
    def scale: Int = 10
    override lazy val computeColumn: IndicatorColumnMapping =
      IndicatorColumnMapping(
        rawComputeColumn.name,
        round(rawComputeColumn.value.cast(DecimalType(16, 10)), scale)
      )
  }
}
