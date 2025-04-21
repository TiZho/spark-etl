package com.github.spark.etl.core.app.processor

import Filter.PairFilter
import com.github.spark.etl.core.app.config.{AppConfig, Env}
import com.github.spark.etl.core.app.config.{AppConfig, Env}

object FilterImplicits {
  implicit class CombineFilters[E <: Env, A, Conf <: AppConfig](x: Filter[A, Conf]) {
    def ++(y: Filter[A, Conf]): Filter[A, Conf] = new PairFilter[A, Conf](x, y)
  }
}
