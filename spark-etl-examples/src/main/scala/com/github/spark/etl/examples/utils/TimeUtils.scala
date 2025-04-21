package com.github.spark.etl.examples.utils

import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object TimeUtils {
  lazy val firstDayOfMonth: Timestamp = {
    val firstDayOfMonth = LocalDateTime.now.withDayOfMonth(1)
    Timestamp.valueOf(atStartOfDay(firstDayOfMonth))
  }

  def atStartOfDay(dateTime: LocalDateTime): LocalDateTime = {
    LocalDateTime.of(
      dateTime.getYear,
      dateTime.getMonth,
      dateTime.getDayOfMonth,
      0, 0, 0
    )
  }

  def toString(ts: LocalDateTime): String = {
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    ts.format(formatter)
  }
}
