package attolini.libs.spark.dbConnector.model

trait Record {
  def toList: scala.collection.immutable.List[String]
}

//case class Parametri(id_scenario: String,
//                     peso_cpi: Float,
//                     peso_tci: Float,
//                     peso_gi: Float,
//                     peso_bpi: Float,
//                     cpi_uguale: Float,
//                     cpi_superiore: Float,
//                     cpi_inferiore: Float,
//                     cpi_autoval: Float,
//                     gi_uguale: Float,
//                     gi_50km: Float,
//                     gi_250km: Float,
//                     gi_oltre: Float,
//                     bpi_div_fp_uguali: Float,
//                     bpi_div_fp_diverse: Float,
//                     bpi_div_uguale_fp_diverse: Float,
//                     bpi_div_diverse_fp_uguale: Float,
//                     allow_anziani_sede_diversa: Boolean,
//                     allow_sede_diversa: Boolean)
//    extends Record {
//  def toList(): List[String] = {
//    List(
//      id_scenario,
//      peso_cpi.toString,
//      peso_tci.toString,
//      peso_gi.toString,
//      peso_bpi.toString,
//      cpi_uguale.toString,
//      cpi_superiore.toString,
//      cpi_inferiore.toString,
//      cpi_autoval.toString,
//      gi_uguale.toString,
//      gi_50km.toString,
//      gi_250km.toString,
//      gi_oltre.toString,
//      bpi_div_fp_uguali.toString,
//      bpi_div_fp_diverse.toString,
//      bpi_div_uguale_fp_diverse.toString,
//      bpi_div_diverse_fp_uguale.toString,
//      allow_anziani_sede_diversa.toString,
//      allow_sede_diversa.toString
//    )
//  }
//}
//
//object Parametri {
//  def apply(cols: Seq[String]): Parametri = {
//    Parametri(
//      cols(0),
//      Ut.toFloat(cols(1)),
//      Ut.toFloat(cols(2)),
//      Ut.toFloat(cols(3)),
//      Ut.toFloat(cols(4)),
//      Ut.toFloat(cols(5)),
//      Ut.toFloat(cols(6)),
//      Ut.toFloat(cols(7)),
//      Ut.toFloat(cols(8)),
//      Ut.toFloat(cols(9)),
//      Ut.toFloat(cols(10)),
//      Ut.toFloat(cols(11)),
//      Ut.toFloat(cols(12)),
//      Ut.toFloat(cols(13)),
//      Ut.toFloat(cols(14)),
//      Ut.toFloat(cols(15)),
//      Ut.toFloat(cols(16)),
//      cols(17).toBoolean,
//      cols(18).toBoolean
//    )
//  }
//}
