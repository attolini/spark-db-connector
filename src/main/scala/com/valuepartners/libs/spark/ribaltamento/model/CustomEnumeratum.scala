package com.valuepartners.libs.spark.ribaltamento.model
import enumeratum._
import enumeratum.EnumEntry

object CustomEnumeratum {

  sealed abstract class SourceLabel(override val entryName: String) extends EnumEntry

  case object Source extends Enum[SourceLabel] {

    case object Hive extends SourceLabel("hive")

    case object MSSql extends SourceLabel("mssql")

    val values = findValues
  }

}
