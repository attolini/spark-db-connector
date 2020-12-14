package com.valuepartners.libs.spark.dbConnector.connector

import java.sql.{DriverManager, Statement}
import java.util.Properties

import com.valuepartners.libs.spark.dbConnector.model.Tables
import com.valuepartners.libs.spark.dbConnector.util.VPLogger
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

class MSSqlServerConnector(implicit sqlC: SQLContext, isVerbose: IsVerbose) extends SparkJDBCHandler with VPLogger {

  val jdbcDriver     = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
  val jdbcSqlConnStr = sqlC.sparkContext.getConf.get("spark.jdbc.server")
  //"jdbc:sqlserver://sql-academy.database.windows.net:1433;database=academy;user=academy@sql-academy;password=20LnrdAcdmy20;loginTimeout=30;"
  Class.forName(jdbcDriver)

  override def load(table: Tables, condition: Option[String], isVerbose: IsVerbose): Either[Throwable, DataFrame] = {
    Class.forName(jdbcDriver)
    Try(
      sqlC.read
        .format("jdbc")
        .options(Map("url" -> jdbcSqlConnStr, "dbtable" -> table.getClass.getSimpleName))
        .load()
        .filter(condition.getOrElse("1=1"))) match {
      case Success(value)          => Right(value)
      case Failure(err: Throwable) => Left(err)
    }
  }

  override def update(table: Tables, values: DataFrame, isVerbose: IsVerbose): Either[Throwable, Unit] = {
    val connectionProperties = new Properties()
    Try(values.write.mode(SaveMode.Overwrite).jdbc(jdbcSqlConnStr, table.getClass.getSimpleName, connectionProperties)) match {
      case Success(value)          => Right(value)
      case Failure(err: Throwable) => Left(err)
    }
  }

  override def delete(table: Tables, condition: Option[String], isVerbose: IsVerbose): Either[Throwable, Unit] = {
    Class.forName(jdbcDriver)
    Try(withResources(DriverManager.getConnection(jdbcSqlConnStr)) { conn =>
      withResources(conn.createStatement) { statement =>
        condition.fold(
          statement.executeUpdate(s"delete from $table")
        ) { f =>
          statement.executeUpdate(s"delete from $table where $f")
        }
      }
    }) match {
      case Success(value)          => Right()
      case Failure(err: Throwable) => Left(err)
    }
  }

  override def write(table: Tables, values: DataFrame, isVerbose: IsVerbose): Either[Throwable, Unit] = {
    val connectionProperties = new Properties()
    Try(values.write.mode(SaveMode.Append).jdbc(jdbcSqlConnStr, table.getClass.getSimpleName, connectionProperties)) match {
      case Success(value)          => Right(value)
      case Failure(err: Throwable) => Left(err)
    }
  }

  override def sql(query: String, isVerbose: IsVerbose) = {
    Class.forName(jdbcDriver)
    withResources(DriverManager.getConnection(jdbcSqlConnStr)) { conn =>
      withResources(conn.createStatement) { statement =>
        val out = statement.executeUpdate(query)
        out
      }
    }
  }

  private def withResources[T <: AutoCloseable, V](r: => T)(f: T => V): V = {
    val resource: T = r
    require(resource != null, "resource is null")
    var exception: Throwable = null
    try {
      f(resource)
    } catch {
      case NonFatal(e) =>
        exception = e
        throw e
    } finally {
      closeAndAddSuppressed(exception, resource)
    }
  }

  private def closeAndAddSuppressed(e: Throwable, resource: AutoCloseable): Unit = {
    if (e != null) {
      try {
        resource.close()
      } catch {
        case NonFatal(suppressed) =>
          e.addSuppressed(suppressed)
      }
    } else {
      resource.close()
    }
  }
}
