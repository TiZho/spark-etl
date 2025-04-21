package com.github.spark.etl.core.app.processor

import Transformer.PairTransformer
import org.apache.spark.sql.Encoder

object TransformerImplicits {
  implicit class CombineTransformers[A: Encoder, B: Encoder](x: Transformer[A, B]) {
    def ++[C: Encoder](y: Transformer[B, C]): Transformer[A, C] = new PairTransformer[A, B, C](x, y)
  }
}
