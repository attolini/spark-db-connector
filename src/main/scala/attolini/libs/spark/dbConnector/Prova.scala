package attolini.libs.spark.dbConnector

import attolini.libs.spark.dbConnector.connector.JdbcServerURL
import attolini.libs.spark.dbConnector.util.{MyLogger, SparkUtil}
//import com.hortonworks.hwc.HiveWarehouseSession
//import com.hortonworks.spark.sql.hive.llap.HiveWarehouseBuilder

object Prova extends MyLogger with SparkUtil {

  def main(args: Array[String]): Unit = {
    implicit val spark     = initSpark
//    implicit val hive      = HiveWarehouseBuilder.session(spark).build
    implicit val isVerbose = spark.conf.get("spark.conf.logger.verbose").toBoolean
    val sourceObject       = spark.conf.get("spark.conf.sourceObject")
    val destinationObject  = spark.conf.get("spark.conf.destinationObject")
    implicit val jdbcUrl   = JdbcServerURL(spark.conf.get("spark.jdbc.server", ""))
    val tablesList         = spark.conf.get("spark.conf.tables").split(",")

//    val sourceConn = getModel(sourceObject)
//    val destinConn = getModel(destinationObject)

    // Errore se la tabella non c'è in destinazione

//    sourceConn.load(Tabella, isVerbose = isVerbose).fold(treatIfErrors(), treatIfSuccess())
  }

//  def getModel(param: String)(implicit
//                              hive: HiveWarehouseSession,
//                              isVerbose: IsVerbose,
//                              jdbcUrl: JdbcServerURL): SparkJDBCHandler = {
//    param match {
//      case Source.Hive.entryName  => new HiveConnector
//      case Source.MSSql.entryName => new MSSqlServerConnector
//      case _                      => new GeneralConnector
//    }
//  }
}

//sorgenteObject=hive
//destinazioneObject=mssql
//tabelle=tab1,tab2,tab3
