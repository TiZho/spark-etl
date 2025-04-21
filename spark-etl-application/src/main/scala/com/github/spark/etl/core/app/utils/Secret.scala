package com.github.spark.etl.core.app.utils

sealed abstract class Secret[T] {
  def keyName: String
  def defaultValue: Option[T]
  def parser: String => T
}

object Secret {
  final case class StringSecret(keyName: String, defaultValue: Option[String] = None)
      extends Secret[String] {
    override def parser: String => String = s => s
  }

  final case class IntSecret(keyName: String, defaultValue: Option[Int] = None)
      extends Secret[Int] {
    override def parser: String => Int = _.toInt
  }

  final case class BooleanSecret(keyName: String, defaultValue: Option[Boolean] = None)
      extends Secret[Boolean] {
    override def parser: String => Boolean = _.toBoolean
  }
}
