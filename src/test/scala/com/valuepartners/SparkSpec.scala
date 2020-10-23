package com.valuepartners

import com.valuepartners.libs.spark.model.Persona
import com.valuepartners.libs.spark.util.Ut
import com.valuepartners.libs.spark.ribaltamento.util.SparkUtil
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class SparkSpec extends AnyWordSpec with Matchers with SparkUtil {

  val (spark, hive) = initSpark

  "populating table test" in {
    import hive.implicits._
    try {
      val tableName = "persona"
      hive.sql(s"create table if not exists $tableName (nome string, cogn string, eta string)")
      hive.sql(s"truncate table $tableName")
      val rdd = Ut.getRddFromFile(spark, "/persona.csv")

      val personaDf = rdd.map(Persona.cast).toDF()
      personaDf.show
      personaDf.registerTempTable("temp")
      hive.sql(s"insert into table $tableName select * from temp")

      val result = hive.sql(s"select * from $tableName")
      val list = result
        .select($"nome")
        .collect
        .mkString("], [")
        .replace("[", "")
        .replace("]", "")

      list shouldEqual "Barack, Leonardo"
    } finally stopSparkHive(spark)
  }
}
