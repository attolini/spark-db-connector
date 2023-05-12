package com.valuepartners.libs.spark.ribaltamento.util

import java.util.{Calendar, Date}

object Ut {

  def getEnv(v: String, ifEmpty: String = null) = {
    val value = System.getenv(v)
    if (value == null)
      (if (ifEmpty == null) "" else ifEmpty)
    else value
  }

  def getTimestampStr(dt: Date = Calendar.getInstance().getTime()) = DateTimeFormat(dt)

  def getTimestamp(): java.sql.Timestamp = {
    new java.sql.Timestamp(Calendar.getInstance().getTimeInMillis)
  }

}
