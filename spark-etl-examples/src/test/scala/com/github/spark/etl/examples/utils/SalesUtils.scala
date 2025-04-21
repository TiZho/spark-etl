package com.github.spark.etl.examples.utils

import com.github.spark.etl.examples.domain.Sale

import java.sql.Timestamp

trait SalesUtils {
 lazy val defaultSale: Sale =
   Sale(
       id = 1,
       customerId = 1,
       customerName = "John",
       customerFirstname = "Doe",
       agentId = 1,
       totalAmount = 1500D,
       articles = List("handbag", "shoes"),
       timestamp = Timestamp.valueOf("2024-10-10 16:20:00")
   )
}
