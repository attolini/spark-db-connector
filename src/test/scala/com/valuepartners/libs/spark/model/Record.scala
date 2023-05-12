package com.valuepartners.libs.spark.model

trait Record {
  def toList: scala.collection.immutable.List[String]
}
