package com.github.spark.etl.core.app.processor

sealed abstract class PartitionGranularity(val name: String, val granularity: Int)

object PartitionGranularity {
  case object Year  extends PartitionGranularity("year", 0)
  case object Month extends PartitionGranularity("month", 1)
  case object Day   extends PartitionGranularity("day", 2)
  case object Hour  extends PartitionGranularity("hour", 3)

  def findHigherGranularity(list: Seq[PartitionGranularity]): Option[Int] =
    list.map(_.granularity).sortWith(_ > _).headOption

}
