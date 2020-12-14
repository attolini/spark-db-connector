package com.valuepartners.libs.spark.dbConnector.util

import java.text.SimpleDateFormat
import java.util.Date

object DateTimeFormat {
  private val FMT = "yyyy-MM-dd HH:mm:ss.SSS"

  def apply(s: String) =
    new SimpleDateFormat(FMT).parse(s)

  def apply(d: Date) =
    new SimpleDateFormat(FMT).format(d)
}
object DateTimeMinutesFormat {
  private val FMT = "yyyyMMddHHmm"

  def apply(s: String) =
    new SimpleDateFormat(FMT).parse(s)

  def apply(d: Date = new Date()): String =
    new SimpleDateFormat(FMT).format(d)
}
object DateFormat {
  private val FMT = "yyyy-MM-dd"

  def apply(s: String) =
    new SimpleDateFormat(FMT).parse(s)

  def apply(d: Date = new Date()) =
    new SimpleDateFormat(FMT).format(d)
}
