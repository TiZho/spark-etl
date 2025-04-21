package com.github.spark.etl.examples.application

import com.github.spark.etl.core.app.application.AbstractApplication.TransformApplication
import com.github.spark.etl.core.app.processor.TransformProcessor
import com.github.spark.etl.examples.config.SalesConfig
import com.github.spark.etl.examples.domain.{RawSale, Sale}
import com.github.spark.etl.examples.processors.SalesProcessor
import pureconfig.generic.auto._

object SalesApplication extends TransformApplication[RawSale, Sale, SalesConfig]{
  lazy val transformProcessor: TransformProcessor[RawSale, Sale, SalesConfig] =
    new SalesProcessor()
}
