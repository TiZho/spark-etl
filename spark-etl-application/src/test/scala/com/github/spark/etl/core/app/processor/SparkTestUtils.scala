package com.github.spark.etl.core.app.processor

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import scala.reflect.runtime.universe._
trait SparkTestUtils {
  def toRow(classInstance: Any, schema: StructType): Row = {
    val values = schema.map { field =>
      val fieldName      = field.name
      val memberTermName = TermName(fieldName)
      val mirror         = scala.reflect.runtime.currentMirror
      val instanceMirror = mirror.reflect(classInstance)
      val fieldMirror = instanceMirror.reflectField(
        instanceMirror.symbol.typeSignature.member(memberTermName).asTerm
      )
      fieldMirror.get
    }
    Row.fromSeq(values)
  }

  def convertListToRows(list: List[_], schema: StructType): List[Row] =
    list.map(element => toRow(element, schema))
}
