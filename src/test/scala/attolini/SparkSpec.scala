package attolini

import attolini.libs.spark.dbConnector.util.{SparkUtil, Ut}
import attolini.libs.spark.model.Persona
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class SparkSpec extends AnyWordSpec with Matchers {

  trait fixture extends SparkUtil {
    val spark = initSpark
  }

  "populating table test" in new fixture {
    import spark.implicits._
    try {
      val tableName = "persona"
      spark.sql(s"create table if not exists $tableName (nome string, cogn string, eta string)")
      spark.sql(s"truncate table $tableName")
      val rdd = Ut.getRddFromFile(spark, "/persona.csv")

      val personaDf = rdd.map(Persona.cast).toDF()
      personaDf.show
      personaDf.createOrReplaceTempView("temp")
      spark.sql(s"insert into table $tableName select * from temp")

      val result = spark.sql(s"select * from $tableName")
      val list = result
        .select($"nome")
        .collect
        .mkString("], [")
        .replace("[", "")
        .replace("]", "")

      list shouldEqual "Barack, Leonardo"
    } finally stopSpark(spark)
  }
}
