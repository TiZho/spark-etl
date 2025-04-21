package com.github.spark.etl.core.app.utils

import scala.reflect.runtime.universe._
import java.sql.Timestamp
import scala.reflect.ClassTag

object ScalaReflectUtils {
  def copyFields[T: ClassTag: TypeTag](source: T, destinations: List[T]): Unit =
    destinations.foreach(dest => copyFieldsTo(source, dest))

  def copyFieldsTo[T: ClassTag: TypeTag](source: T, destination: T): Unit = {
    val sourceMirror              = runtimeMirror(source.getClass.getClassLoader)
    val destinationMirror         = runtimeMirror(destination.getClass.getClassLoader)
    val sourceInstanceMirror      = sourceMirror.reflect(source)
    val destinationInstanceMirror = destinationMirror.reflect(destination)
    val classSymbol               = sourceMirror.classSymbol(source.getClass)
    val fields = classSymbol.toType.members.collect {
      case m: MethodSymbol
          if m.isCaseAccessor
            && (m.returnType <:< typeOf[Timestamp] || m.returnType <:< typeOf[Option[Timestamp]]) =>
        m.asTerm
    }
    fields.foreach { field =>
      destinationInstanceMirror
        .reflectField(field)
        .set(sourceInstanceMirror.reflectField(field).get)
    }
    applyCopyInnerObjects(source, destination)
    applyCopyOptionalInnerObjects(source, destination)
  }

  protected def extractInnerObjects[T: TypeTag: ClassTag](instance: T): List[Any] = {
    val instanceMirror = runtimeMirror(instance.getClass.getClassLoader).reflect(instance)
    val classSymbol    = instanceMirror.symbol
    val fields = classSymbol.toType.members.collect {
      case m: MethodSymbol if m.isCaseAccessor => m
    }
    fields.flatMap { field =>
      val fieldType = field.returnType
      if (
        fieldType <:< typeOf[AnyRef] &&
        fieldType <:< typeOf[Product] &&
        fieldType <:< typeOf[Serializable]
      ) {
        Some(instanceMirror.reflectMethod(field.asMethod).apply())
      } else {
        None
      }
    }.toList
  }

  protected def applyCopyInnerObjects[T: ClassTag: TypeTag](source: T, destination: T): Unit = {
    val innerObjectsSource = extractInnerObjects(source)
    val innerObjectsDest   = extractInnerObjects(destination)
    innerObjectsSource.zip(innerObjectsDest).foreach {
      case (null, d) if d != null =>
        throw new IllegalStateException(
          s"Can not apply copyfield for source=null and destination=$d"
        )
      case (s, null) if s != null =>
        throw new IllegalStateException(
          s"Can not apply copyfield for source=s and destination=null"
        )
      case (s, d) =>
        copyFieldsTo(s, d)
    }
  }

  protected def extractOptionalInnerObjects[T: TypeTag: ClassTag](
      instance: T
    ): List[Option[Any]] = {
    val instanceMirror = runtimeMirror(instance.getClass.getClassLoader).reflect(instance)
    val classSymbol    = instanceMirror.symbol
    val fields = classSymbol.toType.members.collect {
      case m: MethodSymbol if m.isCaseAccessor => m
    }
    fields.flatMap { field =>
      val fieldType = field.returnType
      if (
        fieldType <:< typeOf[Option[AnyRef]] &&
        fieldType <:< typeOf[Option[Product]] &&
        fieldType <:< typeOf[Option[Serializable]]
      ) {
        Some(instanceMirror.reflectMethod(field.asMethod).apply().asInstanceOf[Option[Any]])
      } else {
        None
      }
    }.toList
  }

  protected def applyCopyOptionalInnerObjects[T: ClassTag: TypeTag](
      source: T,
      destination: T
    ): Unit = {
    val innerObjectsSource = extractOptionalInnerObjects(source)
    val innerObjectsDest   = extractOptionalInnerObjects(destination)
    innerObjectsSource.zip(innerObjectsDest).foreach {
      case (Some(s), Some(d)) => copyFieldsTo(s, d)
      case (None, None)       => // Do nothing when both are None
      case _ =>
        throw new IllegalStateException(
          s"Cannot apply copyfield for source=$source and destination=$destination"
        )
    }
  }
}
