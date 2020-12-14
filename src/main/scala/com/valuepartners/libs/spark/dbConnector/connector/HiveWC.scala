package com.valuepartners.libs.spark.dbConnector.connector

import com.hortonworks.spark.sql.hive.llap.HiveWarehouseSession
import com.hortonworks.spark.sql.hive.llap.HiveWarehouseSession.HIVE_WAREHOUSE_CONNECTOR
import com.valuepartners.libs.spark.dbConnector.model.Tables
import com.valuepartners.libs.spark.dbConnector.util.VPLogger
import org.apache.spark.sql.{DataFrame, SaveMode}

import scala.util.{Failure, Success, Try}

class HiveWC(implicit sqlC: HiveWarehouseSession, isVerbose: IsVerbose) extends SparkJDBCHandler with VPLogger {

  override def load(table: Tables, condition: Option[String], isVerbose: IsVerbose): Either[Throwable, DataFrame] = {
    Try(sqlC.executeQuery(s"select * from ${table}").where(condition.getOrElse("1=1"))) match {
      case Success(value) =>
        if (isVerbose.verbose)
          logger.info(s"select * from ${table} " + condition.getOrElse(""))
        Right(value)

      case Failure(err: Throwable) =>
        if (isVerbose.verbose)
          logger.error(s"select * from ${table} " + condition.getOrElse(""))
        Left(err)
    }
  }

  private def hwcWrite(table: Tables, values: DataFrame, mode: SaveMode): Either[Throwable, Unit] = {
    Try(
      values.write
        .format(HIVE_WAREHOUSE_CONNECTOR)
        .option("table", table.getClass.getSimpleName)
        .option("mode", mode.toString)
        .save()) match {
      case Success(_) =>
        if (isVerbose.verbose)
          logger.info(s"Write ${table.getClass.getSimpleName} in ${mode.toString} mode")
        Right()

      case Failure(err: Throwable) =>
        if (isVerbose.verbose)
          logger.error(s"Write ${table.getClass.getSimpleName} in ${mode.toString} mode")
        Left(err)
    }
  }

  override def update(table: Tables, values: DataFrame, isVerbose: IsVerbose): Either[Throwable, Unit] = {
    hwcWrite(table, values, SaveMode.Append)
  }

  override def delete(table: Tables, condition: Option[String], isVerbose: IsVerbose): Either[Throwable, Unit] = {
    Try(sqlC.executeUpdate(s"truncate table ${table}")) match {
      case Success(_) =>
        if (isVerbose.verbose)
          logger.info(s"truncate table ${table}")
        Right()

      case Failure(err: Throwable) =>
        if (isVerbose.verbose)
          logger.error(s"truncate table ${table}")
        Left(err)
    }
  }

  def truncatePartitions(table: Tables, partition: String, partitionValue: String): Either[Throwable, Unit] = {
    Try(sqlC.executeUpdate(s"truncate table ${table} partition ($partition='$partitionValue')")) match {
      case Success(_) =>
        if (isVerbose.verbose)
          logger.info(s"truncate table ${table} partition ($partition='$partitionValue')")
        Right()
      case Failure(err: Throwable) =>
        if (isVerbose.verbose)
          logger.error(s"truncate table ${table} partition ($partition='$partitionValue')")
        Left(err)
    }
  }

  override def write(table: Tables, values: DataFrame, isVerbose: IsVerbose): Either[Throwable, Unit] = {
    hwcWrite(table, values, SaveMode.Overwrite)
  }

  override def sql(query: String, isVerbose: IsVerbose): Any = ???
}
