package attolini.libs.spark.dbConnector.connector

import attolini.libs.spark.dbConnector.util.MyLogger

import java.sql.{DriverManager, Statement}
import org.apache.spark.sql.{DataFrame, SQLContext}

import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

class GeneralConnector(implicit sqlC: SQLContext, jdbcUrl: JdbcServerURL, isLogVerbose: IsLogVerbose)
    extends SparkJDBCHandler
    with MyLogger {

  override val isVerbose = isLogVerbose

  val jdbcDriver = sqlC.sparkContext.getConf.get("spark.jdbc.driver")
  Class.forName(jdbcDriver)
  import sqlC.implicits._

  val jdbcSqlConnStr = jdbcUrl.url

  override def load(table: String, condition: Option[String]): Either[Throwable, DataFrame] = {
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

  override def update(table: String, values: DataFrame): Either[Throwable, Unit] = ???

  override def delete(table: String, condition: Option[String]): Either[Throwable, Unit] = {
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

  override def write(table: String, values: DataFrame): Either[Throwable, Unit] = {
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

  override def sql(query: String) = {
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
