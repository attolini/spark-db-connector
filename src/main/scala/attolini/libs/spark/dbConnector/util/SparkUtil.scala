package attolini.libs.spark.dbConnector.util

import java.nio.file.{Files, Paths}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.{Failure, Properties, Success, Try}

trait SparkMain extends SparkUtil with App {}

trait SparkUtil extends MyLogger {
//  System.setProperty("hadoop.home.dir", "C:/Users/alessio.attolini/Documents/workspace/winutils/hadoop-2.8.3")
  val isLocal = Properties.envOrElse("local", "") != ""

  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)

  def initSpark: SparkSession = {
    Try {
      val conf    = new SparkConf
      val appName = getClass.getSimpleName.replace("$", "")
      conf.setAppName(appName)

      if (isLocal) {
        conf.setMaster("local[*]")
      }

      val spark = SparkSession
        .builder()
        .config(conf)
        .enableHiveSupport()
        .getOrCreate()
      spark.conf.set("hive.exec.dynamic.partition", "true")
      spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
      logger.info(s"Start $appName")
      spark
    } match {
      case Success(_spark)        => _spark
      case Failure(ex: Throwable) => throw ex
    }
  }

  def stopSpark(s: SparkSession): Unit = {
    if (isLocal) {
      Files.delete(Paths.get("./metastore_db/db.lck"))
      Files.delete(Paths.get("./metastore_db/dbex.lck"))
    }
    if (s.sparkContext != null) s.sparkContext.stop()
  }

  def getRddFromResources(sc: SparkContext, path: String): RDD[String] = {
    val filePath = this.getClass.getResource(path).getPath
    sc.textFile(filePath)
  }

}
