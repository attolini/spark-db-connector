package attolini.libs.spark.dbConnector.connector

import attolini.libs.spark.dbConnector.util.MyLogger
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

import scala.util.{Failure, Success, Try}

class HiveConnector(implicit sqlC: SQLContext, isLogVerbose: IsLogVerbose) extends SparkJDBCHandler with MyLogger {

  override val isVerbose = isLogVerbose

  override def load(table: String, condition: Option[String]): Either[Throwable, DataFrame] = {
    Try(sqlC.sql(s"select * from ${table} where ${condition.getOrElse("1=1")}")) match {
      case Success(value)          => Right(value)
      case Failure(err: Throwable) => Left(err)
    }
  }

  override def update(table: String, values: DataFrame): Either[Throwable, Unit] = {
    Try(values.write.mode(SaveMode.Overwrite).insertInto(table.getClass.getSimpleName)) match {
      case Success(value)          => Right(value)
      case Failure(err: Throwable) => Left(err)
    }
  }

  override def delete(table: String, condition: Option[String]): Either[Throwable, Unit] = {
    Try(sqlC.sql(s"truncate table ${table} where ${condition.getOrElse("1=1")}")) match {
      case Success(_)              => Right()
      case Failure(err: Throwable) => Left(err)
    }
  }

  def truncatePartitions(table: String, partition: String, partitionValue: String): Either[Throwable, Unit] = {
    Try(sqlC.sql(s"truncate table ${table} partition ($partition='$partitionValue')")) match {
      case Success(_)              => Right()
      case Failure(err: Throwable) => Left(err)
    }
  }

  override def write(table: String, values: DataFrame): Either[Throwable, Unit] = {
    Try(values.write.insertInto(table.getClass.getSimpleName)) match {
      case Success(value)          => Right(value)
      case Failure(err: Throwable) => Left(err)
    }
  }

  def writePartitions(table: String,
                      df: DataFrame,
                      saveMode: SaveMode = SaveMode.Append,
                      partition: String,
                      partitionValue: String) = {
    if (isVerbose.verbose) df.show()
    Try(truncatePartitions(table, partition, partitionValue)) match {
      case Success(_) =>
        if (isVerbose.verbose) logger.info(s"Partition ${partitionValue} truncated in ${table}")
        Try(df.write.mode(saveMode).partitionBy(partition).insertInto(table.getClass.getSimpleName)) match {
          case Success(_) =>
            if (isVerbose.verbose) logger.info(s"Write succeeded on ${table}")
            Right()
          case Failure(err: Throwable) => Left(err)
        }
      case Failure(err: Throwable) => Left(err)
    }
  }

  override def sql(query: String): Either[Throwable, DataFrame] =
    Try(sqlC.sql(query)) match {
      case Success(df)             => Right(df)
      case Failure(err: Throwable) => Left(err)
    }
}
