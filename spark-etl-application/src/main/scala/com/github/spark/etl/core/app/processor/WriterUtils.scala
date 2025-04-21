package com.github.spark.etl.core.app.processor

import java.sql.Date
import java.text.SimpleDateFormat

object WriterUtils {
  sealed trait OperatorPredicate {
    def symbol: String
  }

  case object EqualOperator extends OperatorPredicate {
    override val symbol: String = "=="
  }

  case object InfOperator extends OperatorPredicate {
    override val symbol: String = "<"
  }

  case object InfOrEqOperator extends OperatorPredicate {
    override val symbol: String = "<="
  }

  case object SupOperator extends OperatorPredicate {
    override val symbol: String = ">"
  }

  case object SupOrEqOperator extends OperatorPredicate {
    override val symbol: String = ">="
  }

  trait AbstractPredicate {
    def buildCondition: String
  }

  final case class Predicate[T](columnName: String, op: OperatorPredicate, value: T)
      extends AbstractPredicate {

    private val sdf = new SimpleDateFormat("yyyy-MM-dd")

    override def buildCondition: String = value match {
      case v if v.isInstanceOf[String] =>
        s"""$columnName ${op.symbol} '$v'"""
      case v if v.isInstanceOf[Date] =>
        val formattedDate = sdf.format(v)
        s"""$columnName ${op.symbol} date('$formattedDate')"""
      case v =>
        s"""$columnName ${op.symbol} $v"""
    }
  }

  object Predicate {
    def apply[T](columnName: String, value: T): Predicate[T] =
      Predicate(columnName, EqualOperator, value)
  }

  sealed trait Conjunction {
    def symbol: String
  }

  case object AndConjunction extends Conjunction {
    override val symbol: String = "and"
  }

  case object OrConjunction extends Conjunction {
    override val symbol: String = "or"
  }

  final case class PairPredicate(
      p1: AbstractPredicate,
      p2: AbstractPredicate,
      conjunction: Conjunction)
      extends AbstractPredicate {
    override def buildCondition: String =
      s"${p1.buildCondition} ${conjunction.symbol} ${p2.buildCondition}"
  }

  implicit class CombinePredicate(p1: AbstractPredicate) {
    def and(p2: AbstractPredicate): PairPredicate = PairPredicate(p1, p2, AndConjunction)

    def or(p2: AbstractPredicate): PairPredicate = PairPredicate(p1, p2, OrConjunction)
  }

}
