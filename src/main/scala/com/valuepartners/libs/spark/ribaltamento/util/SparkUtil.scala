package com.valuepartners.libs.spark.ribaltamento.util

import java.nio.file.{Files, Paths}

import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Properties

trait SparkUtil {

  val isLocal = Properties.envOrElse("local", "") != ""

  def initSpark = {
    val isLocal = Properties.envOrElse("local", "") != ""
    val appName = getClass.getSimpleName
    val conf    = new SparkConf().setAppName(appName).setMaster("local[*]")
    import org.apache.spark.sql.hive._
    val sc   = new SparkContext(conf)
    val hive = new HiveContext(sc)
    if (isLocal) {
      hive.setConf("hive.metastore.warehouse.dir", getClass.getResource(".").toString)
    }
    hive.setConf("hive.exec.dynamic.partition", "true")
    hive.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

    sc.setLogLevel("ERROR")
    (sc, hive)
  }

  def stopSpark(sc: SparkContext) = if (sc != null) sc.stop()

  def stopSparkHive(sc: SparkContext): Unit = {
    if (isLocal) {
      try {
        Files.delete(Paths.get("./metastore_db/db.lck"))
      }
      try {
        Files.delete(Paths.get("./metastore_db/dbex.lck"))
      }
    }
    stopSpark(sc)
  }
}
