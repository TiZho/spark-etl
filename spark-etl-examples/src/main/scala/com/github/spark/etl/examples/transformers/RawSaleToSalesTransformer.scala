package com.github.spark.etl.examples.transformers

import com.github.spark.etl.core.app.processor.CustomEncoders.customEncoder
import com.github.spark.etl.core.app.processor.Transformer.MappingTransformer
import com.github.spark.etl.examples.domain.{RawSale, Sale}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, to_timestamp}

import java.sql.Timestamp
import scala.collection.immutable.ListMap

class RawSaleToSalesTransformer
  extends MappingTransformer[RawSale, Sale] {
  override def mapping(input: RawSale): Sale =
    Sale(id = input.id,
      customerId = input.customerId,
      customerName = input.customerName,
      customerFirstname = input.customerFirstname,
      agentId = input.agentId,
      totalAmount = input.totalAmount,
      articles = input.articles,
      timestamp = Timestamp.valueOf(input.timestamp_str)
    )

  override def mappingCols(): ListMap[String, Column] =
    ListMap(
        Sale.idCol -> col(RawSale.idCol),
        Sale.customerIdCol -> col(RawSale.customerIdCol),
        Sale.customerNameCol -> col(RawSale.customerNameCol),
        Sale.customerFirstnameCol -> col(RawSale.customerFirstnameCol),
        Sale.agentIdCol -> col(RawSale.agentIdCol),
        Sale.totalAmountCol -> col(RawSale.totalAmountCol),
        Sale.articlesCol -> col(RawSale.articlesCol),
        Sale.timestampCol -> to_timestamp(col(RawSale.timestamp_strCol), "dd-MM-yyyy HH:mm")
    )
}

