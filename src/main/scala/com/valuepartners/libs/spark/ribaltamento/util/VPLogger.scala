package com.valuepartners.libs.spark.ribaltamento.util

import org.slf4j.LoggerFactory

trait VPLogger {
  val logger = LoggerFactory.getLogger(this.getClass)
}
