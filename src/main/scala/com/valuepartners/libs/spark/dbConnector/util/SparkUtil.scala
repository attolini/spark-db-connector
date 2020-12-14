package com.vp.sparkhive.util

import java.nio.file.{Files, Paths}

import com.valuepartners.libs.spark.dbConnector.util.VPLogger
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.{Failure, Properties, Success, Try}

trait SparkMain extends SparkUtil with App {}

trait SparkUtil extends VPLogger {
  val isLocal = Properties.envOrElse("local", "") != ""

  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)

  def initSpark = {
    Try {
      val conf    = new SparkConf
      val appName = getClass.getSimpleName
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

  def stopSparkHive(sc: SparkContext): Unit = {
    if (isLocal) {
      Files.delete(Paths.get("./metastore_db/db.lck"))
      Files.delete(Paths.get("./metastore_db/dbex.lck"))
    }
    if (sc != null) sc.stop()
  }

  def getRddFromResources(sc: SparkContext, path: String) = {
    val filePath = this.getClass.getResource(path).getPath
    sc.textFile(filePath)
  }

}
