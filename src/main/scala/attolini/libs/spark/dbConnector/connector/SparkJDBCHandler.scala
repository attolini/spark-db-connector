package attolini.libs.spark.dbConnector.connector

import org.apache.spark.sql.DataFrame

case class IsLogVerbose(verbose: Boolean = false)

trait SparkJDBCHandler {

  val isVerbose: IsLogVerbose

  def load(table: String, condition: Option[String] = None): Either[Throwable, DataFrame]

  def update(table: String, values: DataFrame): Either[Throwable, Unit]

  def delete(table: String, condition: Option[String] = None): Either[Throwable, Unit]

  def write(table: String, values: DataFrame): Either[Throwable, Unit]

  def sql(query: String): Any
}
