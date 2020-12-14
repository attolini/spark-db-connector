package com.valuepartners.libs.spark.ribaltamento.connector

import com.valuepartners.libs.spark.ribaltamento.model.Tables
import org.apache.spark.sql.DataFrame

case class IsVerbose(verbose: Boolean = false)

trait SparkJDBCHandler {

  def load(table: Tables, condition: Option[String] = None, isVerbose: IsVerbose): Either[Throwable, DataFrame]

  def update(table: Tables, values: DataFrame, isVerbose: IsVerbose): Either[Throwable, Unit]

  def delete(table: Tables, condition: Option[String] = None, isVerbose: IsVerbose): Either[Throwable, Unit]

  def write(table: Tables, values: DataFrame, isVerbose: IsVerbose): Either[Throwable, Unit]

  def sql(query: String, isVerbose: IsVerbose): Any
}
