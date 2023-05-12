package attolini.libs.spark.dbConnector.util

import org.slf4j.LoggerFactory

trait MyLogger {
  val logger = LoggerFactory.getLogger(this.getClass)
}
