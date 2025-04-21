package com.github.spark.etl.examples.domain

case class RawSale(
                  id: Int,
                  customerId: Int,
                  customerName: String,
                  customerFirstname: String,
                  agentId: Int,
                  totalAmount: Double,
                  articles: List[String],
                  timestamp_str: String)


object RawSale {
  val idCol = "id"
  val customerIdCol = "customerId"
  val customerNameCol = "customerName"
  val customerFirstnameCol = "customerFirstname"
  val agentIdCol = "agentId"
  val totalAmountCol = "totalAmount"
  val articlesCol = "articles"
  val timestamp_strCol = "timestamp_str"
}