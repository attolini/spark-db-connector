package com.valuepartners.libs.spark.ribaltamento

import com.valuepartners.libs.spark.ribaltamento.connector._
import com.valuepartners.libs.spark.ribaltamento.util.{SparkUtil, VPLogger}
import org.apache.spark.sql.hive.HiveContext

object Prova extends VPLogger with SparkUtil {

  def main(args: Array[String]): Unit = {
    implicit val (spark, hive) = initSpark
    implicit val isVerbose     = IsVerbose(spark.getConf.get("spark.conf.logger.verbose").toBoolean)
    val sourceObject           = spark.getConf.get("spark.conf.sourceObject")
    val destinationObject      = spark.getConf.get("spark.conf.destinationObject")
    implicit val jdbcUrl       = JdbcServerURL(spark.getConf.get("spark.jdbc.server", ""))
    val tablesList             = spark.getConf.get("spark.conf.tables").split(",")

    val sourceConn = getModel(sourceObject)
    val destinConn = getModel(destinationObject)

    // Errore se la tabella non c'è in destinazione

//    sourceConn.load(Tabella, isVerbose = isVerbose).fold(treatIfErrors(), treatIfSuccess())
  }

  def getModel(param: String)(implicit
                              hive: HiveContext,
                              isVerbose: IsVerbose,
                              jdbcUrl: JdbcServerURL): SparkJDBCHandler = {
    param match {
      case "hive"  => new HiveConnector
      case "mssql" => new MSSqlServerConnector
      case _       => new GeneralConnector
    }
  }
}

//sorgenteObject=hive
//destinazioneObject=mssql
//tabelle=tab1,tab2,tab3
