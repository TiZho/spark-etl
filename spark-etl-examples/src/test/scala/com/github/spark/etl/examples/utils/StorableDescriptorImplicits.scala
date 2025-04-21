package com.github.spark.etl.examples.utils

import com.github.spark.etl.core.app.processor.StorableDescriptor
import com.github.spark.etl.core.app.processor.StorableDescriptor.BaseStorableDescriptor
import com.github.spark.etl.examples.domain.{RawSale, Sale}

object StorableDescriptorImplicits extends BaseStorableDescriptor {
  override implicit val customStorableDescriptors: Seq[StorableDescriptor[_ <: Product]] =
    Seq(
      defaultStorableDescriptor[RawSale],
      defaultStorableDescriptor[Sale]
    )
}