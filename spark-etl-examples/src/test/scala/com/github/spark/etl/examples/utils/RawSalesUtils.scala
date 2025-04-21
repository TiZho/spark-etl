package com.github.spark.etl.examples.utils

import com.github.spark.etl.examples.domain.RawSale

trait RawSalesUtils {
  lazy val defaultRawSale: RawSale =
    RawSale(
      id = 1,
      customerId = 1,
      customerName = "John",
      customerFirstname = "Doe",
      agentId = 1,
      totalAmount = 1500D,
      articles = List("handbag", "shoes"),
      timestamp_str = "2024-10-10 16:20:00"
    )
}
