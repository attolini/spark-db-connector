package attolini.libs.spark.dbConnector.util

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import java.nio.file.NoSuchFileException
import java.util.{Calendar, Date}
import scala.util.{Failure, Success, Try}

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

  def getRddFromFile(s: SparkSession, path: String): RDD[String] = {
    Try {
      val filePath = this.getClass.getResource(path).getPath
      s.sparkContext.textFile(filePath)
    } match {
      case Success(rdd) => rdd
      case Failure(err) => throw new NoSuchFileException(err.getMessage)
    }
  }
}
