package com.github.spark.etl.core.app.utils

import scala.util.Try

object SimpleJsonParser {
  sealed trait JsonValue
  case class JsonString(value: String)   extends JsonValue
  case class JsonNumber(value: Int)      extends JsonValue
  case class JsonBoolean(value: Boolean) extends JsonValue
  case object JsonNull                   extends JsonValue

  def parse(json: String): Option[Map[String, JsonValue]] = Try {
    val keyValuePairs = json
      .split(",")
      .map(_.trim)
      .map(_.filter(x => !Array('{', '}').contains(x)))
      .map { pair =>
        val Array(key, value) = pair.split(":").map(_.trim)
        val jsonValue = value.head match {
          case '"'                                    => JsonString(value.drop(1).dropRight(1))
          case 't'                                    => JsonBoolean(true)
          case 'f'                                    => JsonBoolean(false)
          case 'n'                                    => JsonNull
          case c if c.isDigit || c == '-' || c == '.' => JsonNumber(value.toInt)
          case _ => throw new IllegalArgumentException("Invalid JSON")
        }
        (key.drop(1).dropRight(1), jsonValue)
      }
    keyValuePairs.toMap
  }.toOption

  def extractString(expression: String, key: String): Option[String] =
    parse(expression).flatMap(_.get(key)).collect { case JsonString(v) => v }

  def extractNumber(expression: String, key: String): Option[Int] =
    parse(expression).flatMap(_.get(key)).collect { case JsonNumber(v) => v }

  def extractBoolean(expression: String, key: String): Option[Boolean] =
    parse(expression).flatMap(_.get(key)).collect { case JsonBoolean(v) => v }

}
