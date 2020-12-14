package com.valuepartners.libs.spark.ribaltamento.connector

import com.valuepartners.libs.spark.ribaltamento.model.Tables
import com.valuepartners.libs.spark.ribaltamento.util.VPLogger
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

import scala.util.{Failure, Success, Try}

class HiveConnector(implicit sqlC: SQLContext, isVerbose: IsVerbose) extends SparkJDBCHandler with VPLogger {

  override def load(table: Tables, condition: Option[String], verbose: IsVerbose): Either[Throwable, DataFrame] = {
    Try(sqlC.sql(s"select * from ${table} where ${condition.getOrElse("1=1")}")) match {
      case Success(value)          => Right(value)
      case Failure(err: Throwable) => Left(err)
    }
  }

  override def update(table: Tables, values: DataFrame, isVerbose: IsVerbose): Either[Throwable, Unit] = {
    Try(values.write.mode(SaveMode.Overwrite).insertInto(table.getClass.getSimpleName)) match {
      case Success(value)          => Right(value)
      case Failure(err: Throwable) => Left(err)
    }
  }

  override def delete(table: Tables, condition: Option[String], isVerbose: IsVerbose): Either[Throwable, Unit] = {
    Try(sqlC.sql(s"truncate table ${table} where ${condition.getOrElse("1=1")}")) match {
      case Success(_)              => Right()
      case Failure(err: Throwable) => Left(err)
    }
  }

  def truncatePartitions(table: Tables, partition: String, partitionValue: String): Either[Throwable, Unit] = {
    Try(sqlC.sql(s"truncate table ${table} partition ($partition='$partitionValue')")) match {
      case Success(_)              => Right()
      case Failure(err: Throwable) => Left(err)
    }
  }

  override def write(table: Tables, values: DataFrame, isVerbose: IsVerbose): Either[Throwable, Unit] = {
    Try(values.write.insertInto(table.getClass.getSimpleName)) match {
      case Success(value)          => Right(value)
      case Failure(err: Throwable) => Left(err)
    }
  }

  def writePartitions(table: Tables,
                      df: DataFrame,
                      saveMode: SaveMode = SaveMode.Append,
                      partition: String,
                      partitionValue: String,
                      isVerbose: IsVerbose) = {
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

  override def sql(query: String, isVerbose: IsVerbose): Either[Throwable, DataFrame] =
    Try(sqlC.sql(query)) match {
      case Success(df)             => Right(df)
      case Failure(err: Throwable) => Left(err)
    }
}
