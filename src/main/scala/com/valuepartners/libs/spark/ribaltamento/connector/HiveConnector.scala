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
      case Success(value)          => Right()
      case Failure(err: Throwable) => Left(err)
    }
  }

  def truncatePartitions(table: Tables, partition: String, partitionValue: String): Either[Throwable, Unit] = {
    Try(sqlC.sql(s"truncate table ${table} partition ($partition='$partitionValue')")) match {
      case Success(value)          => Right()
      case Failure(err: Throwable) => Left(err)
    }
  }

  override def write(table: Tables, values: DataFrame, isVerbose: IsVerbose): Either[Throwable, Unit] = {
    Try(values.write.insertInto(table.getClass.getSimpleName)) match {
      case Success(value)          => Right(value)
      case Failure(err: Throwable) => Left(err)
    }
  }

  def writePartitions(table: String,
                      df: DataFrame,
                      saveMode: SaveMode = SaveMode.Append,
                      partitionKey: Option[String] = None,
                      isVerbose: IsVerbose) = {
    if (isVerbose.verbose) {
      logger.info(s"Saving df for table ${table}, save mode is ${saveMode}")
      df.show()
    }
    partitionKey.fold(
      df.write.mode(saveMode).insertInto(table)
    )(key => df.write.mode(saveMode).partitionBy(key).insertInto(table))
  }

  override def executeQuery(query: String, isVerbose: IsVerbose): Either[Throwable, DataFrame] =
    Try(sqlC.sql(query)) match {
      case Success(value)          => Right(value)
      case Failure(err: Throwable) => Left(err)
    }
}
