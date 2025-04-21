package com.github.spark.etl

import monocle.Lens

import scala.language.experimental.macros
import scala.reflect.macros.blackbox.Context

object LensMacro {
  def updateProperty[A, B](obj: A, lens: Lens[A, B], newValue: B): A =
    macro updatePropertyImpl[A, B]

  def updatePropertyImpl[A: c.WeakTypeTag, B: c.WeakTypeTag](
      c: Context
    )(obj: c.Expr[A],
      lens: c.Expr[Lens[A, B]],
      newValue: c.Expr[B]
    ): c.Expr[A] = {

    import c.universe._

    // Generate AST for the lens.set(obj)(newValue)
    val updateExpr = q"$lens.set($newValue)($obj)"

    // Typecheck and return the result
    c.Expr[A](c.typecheck(updateExpr))
  }
}
