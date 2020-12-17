package com.valuepartners.libs.spark.dbConnector.connector

import java.sql.{DriverManager, Statement}

import com.valuepartners.libs.spark.dbConnector.model.Tables
import com.valuepartners.libs.spark.dbConnector.util.VPLogger
import org.apache.spark.sql.{DataFrame, SQLContext}

import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

class GeneralConnector(implicit sqlC: SQLContext, jdbcUrl: JdbcServerURL, verbose: IsVerbose)
    extends SparkJDBCHandler
    with VPLogger {

  val jdbcDriver = sqlC.sparkContext.getConf.get("spark.jdbc.driver")
  Class.forName(jdbcDriver)
  import sqlC.implicits._

  val jdbcSqlConnStr = jdbcUrl.url

  override def load(table: Tables, condition: Option[String], isVerbose: IsVerbose): Either[Throwable, DataFrame] = {
    Try(withResources(DriverManager.getConnection(jdbcSqlConnStr)) { conn =>
      val out = withResources(conn.createStatement) { statement =>
        val rs = statement.executeQuery(s"SELECT * FROM $table WHERE ${condition.fold("1=1")(identity)}")
        new Iterator[List[String]] {
          def hasNext = rs.next()

          def next() = (for { i <- 0 until rs.getMetaData.getColumnCount } yield rs.getString(i + 1)).toList
        }.toList
      }
      out.toDF()
    }) match {
      case Success(value)          => Right(value)
      case Failure(err: Throwable) => Left(err)
    }
  }

  override def update(table: Tables, values: DataFrame, isVerbose: IsVerbose): Either[Throwable, Unit] = ???

  override def delete(table: Tables, condition: Option[String], isVerbose: IsVerbose) = {
    Try(withResources(DriverManager.getConnection(jdbcSqlConnStr)) { conn =>
      withResources(conn.createStatement) { statement =>
        condition.fold(
          statement.executeUpdate(s"truncate $table")
        ) { f =>
          statement.executeUpdate(s"delete from $table where $f")
        }
      }
    }) match {
      case Success(value)          => Right(value)
      case Failure(err: Throwable) => Left(err)
    }
  }

  override def write(table: Tables, values: DataFrame, isVerbose: IsVerbose) = {
    Try(withResources(DriverManager.getConnection(jdbcSqlConnStr)) { conn =>
      assert(values.count() != 0)
      val numberOfColumns = values.head.size
      val questionsMarks  = (for (i <- 0 until numberOfColumns) yield "?").mkString(",")
      val SQL             = s"insert into $table values(${questionsMarks})"

      withResources(conn.prepareStatement(SQL, Statement.RETURN_GENERATED_KEYS)) { statement =>
        values.foreach { rec =>
          for (i <- 0 until numberOfColumns) statement.setString(i + 1, rec(i).toString)
          statement.addBatch()
        }
        statement.executeBatch.sum
      }
    }) match {
      case Success(value)          => Right()
      case Failure(err: Throwable) => Left(err)
    }
  }

  override def sql(query: String, isVerbose: IsVerbose) = {
    withResources(DriverManager.getConnection(jdbcSqlConnStr)) { conn =>
      withResources(conn.createStatement) { statement =>
        statement.executeUpdate(query)
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
