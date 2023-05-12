package attolini.libs.spark.model

trait Record {
  def toList: scala.collection.immutable.List[String]
}
