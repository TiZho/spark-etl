package com.github.spark.etl.examples.domain

import java.sql.Timestamp

case class Sale(id: Int,
                customerId: Int,
                customerName: String,
                customerFirstname: String,
                agentId: Int,
                totalAmount: Double,
                articles: List[String],
                timestamp: Timestamp)

object Sale {
  val idCol = "id"
  val customerIdCol = "customerId"
  val customerNameCol = "customerName"
  val customerFirstnameCol = "customerFirstname"
  val agentIdCol = "agentId"
  val totalAmountCol = "totalAmount"
  val articlesCol = "articles"
  val timestampCol = "timestamp"
}