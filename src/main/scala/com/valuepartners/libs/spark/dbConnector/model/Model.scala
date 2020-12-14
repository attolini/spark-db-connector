package com.valuepartners.libs.spark.dbConnector.model

import com.valuepartners.libs.spark.dbConnector.connector.SparkJDBCHandler
import javax.inject.Inject

sealed trait Tables

case object Tabella extends Tables
// creare case object per tutte le tabelle di Tables

class Model @Inject()(val handler: SparkJDBCHandler) {
//  private val filter = s"id_scenario = '${solution.scenarioId}'"
  //
  //  lazy val compatibilityIndexes = MatchingIndexes(
  //    handler
  //      .load(RISULTATIMI_SHORT, Some(filter))
  //      .asInstanceOf[List[RisultatiMICompressed]])
}
