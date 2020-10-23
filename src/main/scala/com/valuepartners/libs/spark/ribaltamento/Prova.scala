package com.valuepartners.libs.spark.ribaltamento

import akka.util.Helpers.Requiring
import com.valuepartners.libs.spark.ribaltamento.connector._
import com.valuepartners.libs.spark.ribaltamento.util.{SparkUtil, VPLogger}
import org.apache.spark.sql.SQLContext

object Prova extends VPLogger with SparkUtil {

  def main(args: Array[String]): Unit = {
    implicit val (spark, sqlC)     = initSpark
    implicit val isVerbose         = IsVerbose(spark.getConf.get("spark.conf.logger.verbose").toBoolean)
    implicit val sourceObject      = spark.getConf.get("spark.conf.sourceObject")
    implicit val destinationObject = spark.getConf.get("spark.conf.destinationObject")
    implicit val jdbcUrl           = JdbcServerURL(spark.getConf.get("spark.jdbc.server", ""))
    val tablesList                 = spark.getConf.get("spark.conf.tables").split(",")

    val sourceConn = getModel
    val destinConn = getModel
    // Errore se la tabella non c'è in destinazione
  }

  def getModel(implicit param: String,
               sqlC: SQLContext,
               isVerbose: IsVerbose,
               jdbcUrl: JdbcServerURL): SparkJDBCHandler = {
    param match {
      case "hive"    => new HiveConnector
      case "mssql"   => new MSSqlServerConnector
      case "phoenix" => ???
      case _         => new GeneralConnector
    }
  }
}

//sorgenteObject=hive
//destinazioneObject=mssql
//tabelle=tab1,tab2,tab3
