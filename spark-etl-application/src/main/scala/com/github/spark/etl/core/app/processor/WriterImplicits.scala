package com.github.spark.etl.core.app.processor

import Writer.PairWriter

object WriterImplicits {
  implicit class MultipleWriter[O](x: Writer[O]) {
    def ++(y: Writer[O]): Writer[O] = new PairWriter[O](x, y)
  }
}
