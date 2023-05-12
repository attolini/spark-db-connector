//package attolini.libs.spark.dbConnector.connector
//
//import attolini.libs.spark.dbConnector.model.Tables
//import attolini.libs.spark.dbConnector.util.MyLogger
//import com.hortonworks.spark.sql.hive.llap.HiveWarehouseSession
//import com.hortonworks.spark.sql.hive.llap.HiveWarehouseSession.HIVE_WAREHOUSE_CONNECTOR
//import org.apache.spark.sql.{DataFrame, SaveMode}
//
//import scala.util.{Failure, Success, Try}
//
//class HiveWC(implicit sqlC: HiveWarehouseSession, isLogVerbose: IsLogVerbose) extends SparkJDBCHandler with MyLogger {
//
//  override val isVerbose = isLogVerbose
//
//  override def load(table: String, condition: Option[String]): Either[Throwable, DataFrame] = {
//    Try(sqlC.executeQuery(s"select * from ${table}").where(condition.getOrElse("1=1"))) match {
//      case Success(value) =>
//        if (isVerbose.verbose)
//          logger.info(s"select * from ${table} " + condition.getOrElse(""))
//        Right(value)
//
//      case Failure(err: Throwable) =>
//        if (isVerbose.verbose)
//          logger.error(s"select * from ${table} " + condition.getOrElse(""))
//        Left(err)
//    }
//  }
//
//  private def hwcWrite(table: String, values: DataFrame, mode: SaveMode): Either[Throwable, Unit] = {
//    Try(
//      values.write
//        .format(HIVE_WAREHOUSE_CONNECTOR)
//        .option("table", table)
//        .option("mode", mode.toString)
//        .save()) match {
//      case Success(_) =>
//        if (isVerbose.verbose)
//          logger.info(s"Write $table in ${mode.toString} mode")
//        Right()
//
//      case Failure(err: Throwable) =>
//        if (isVerbose.verbose)
//          logger.error(s"Write $table in ${mode.toString} mode")
//        Left(err)
//    }
//  }
//
//  override def update(table: String, values: DataFrame): Either[Throwable, Unit] = {
//    hwcWrite(table, values, SaveMode.Append)
//  }
//
//  override def write(table: String, values: DataFrame): Either[Throwable, Unit] = {
//    hwcWrite(table, values, SaveMode.Overwrite)
//  }
//
//  def writePartition(
//                      table: String,
//                      values: DataFrame,
//                      partition: String,
//                      mode: SaveMode = SaveMode.Overwrite): Either[Throwable, Unit] = {
//    Try(
//      values.write
//        .format(HIVE_WAREHOUSE_CONNECTOR)
//        .option("table", table)
//        .option("partition", partition)
//        .mode(mode)
//        .save()) match {
//      case Success(_) =>
//        if (isVerbose.verbose)
//          logger.info(s"Write $table ")
//        Right()
//
//      case Failure(err: Throwable) =>
//        if (isVerbose.verbose)
//          logger.error(s"Write $table ")
//        Left(err)
//    }
//  }
//
//  override def delete(table: String, condition: Option[String]): Either[Throwable, Unit] = {
//    Try(sqlC.executeUpdate(s"truncate table ${table}")) match {
//      case Success(_) =>
//        if (isVerbose.verbose)
//          logger.info(s"truncate table ${table}")
//        Right()
//
//      case Failure(err: Throwable) =>
//        if (isVerbose.verbose)
//          logger.error(s"truncate table ${table}")
//        Left(err)
//    }
//  }
//
//  def truncatePartitions(table: Tables, partition: String, partitionValue: String): Either[Throwable, Unit] = {
//    Try(sqlC.executeUpdate(s"truncate table ${table} partition ($partition='$partitionValue')")) match {
//      case Success(_) =>
//        if (isVerbose.verbose)
//          logger.info(s"truncate table ${table} partition ($partition='$partitionValue')")
//        Right()
//      case Failure(err: Throwable) =>
//        if (isVerbose.verbose)
//          logger.error(s"truncate table ${table} partition ($partition='$partitionValue')")
//        Left(err)
//    }
//  }
//
//  override def sql(query: String): Any = {
//    Try(sqlC.executeQuery(query)) match {
//      case Success(value) =>
//        if (isVerbose.verbose)
//          logger.info(query.substring(0, 15) + " done")
//        Right(value)
//
//      case Failure(err: Throwable) =>
//        if (isVerbose.verbose)
//          logger.error(query.substring(0, 15) + " failed")
//        Left(err)
//    }
//  }
//
//    def setDatabase(db: String) =
//      Try(sqlC.setDatabase(db)) match {
//        case Success(value) =>
//          if (isVerbose.verbose)
//            logger.info(s"Database $db has been set")
//          Right(value)
//
//        case Failure(err: Throwable) =>
//          if (isVerbose.verbose)
//            logger.error(s"Database $db has NOT been set")
//          Left(err)
//      }
//
//    def drop(table: String): Either[Throwable, Any] = {
//      Try(sqlC.executeUpdate(s"drop table $table")) match {
//        case Success(boo: Boolean) =>
//          if (isVerbose.verbose) logger.info(s"Table $table deleted")
//          Right(boo)
//
//        case Failure(err: Throwable) =>
//          if (isVerbose.verbose) logger.error(s"Table $table NOT deleted")
//          Left(err)
//      }
//    }
//
//}
