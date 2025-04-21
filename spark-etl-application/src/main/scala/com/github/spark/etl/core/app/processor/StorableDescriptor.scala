package com.github.spark.etl.core.app.processor

import StorableDescriptor.getFieldNames
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.types.StructType

import scala.reflect.runtime.universe._

abstract class StorableDescriptor[T <: Product: TypeTag] {
  def columnIds: Seq[String]
  def partitionCols: Seq[String]
  def readOptions: Map[String, String]  = Map()
  def writeOptions: Map[String, String] = Map()
  def schema: StructType                = Encoders.product[T].schema
  def fields: Seq[String]               = getFieldNames[T]
  def typetag: TypeTag[T]               = typeTag[T]
  def clazz: Class[T] = {
    val mirror = runtimeMirror(getClass.getClassLoader)
    mirror.runtimeClass(typetag.tpe).asInstanceOf[Class[T]]
  }
}

object StorableDescriptor {
  private def getFieldNames[T: TypeTag]: List[String] =
    typeOf[T].members.collect {
      case m: MethodSymbol if m.isCaseAccessor => m.name.toString
    }.toList

  abstract class BaseStorableDescriptor {
    implicit def defaultStorableDescriptor[I <: Product: TypeTag]: StorableDescriptor[I] =
      new StorableDescriptor[I] {
        override def columnIds: Seq[String]     = Seq()
        override def partitionCols: Seq[String] = Seq()
      }
    implicit def customStorableDescriptors: Seq[StorableDescriptor[_]]
  }

}
