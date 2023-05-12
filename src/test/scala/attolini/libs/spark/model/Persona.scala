package attolini.libs.spark.model

object Persona {
  def apply(cols: List[String]): Persona = Persona(
    cols(0),
    cols(1),
    cols(2)
  )
  val cast = (rawLine: String) => {
    val columns = rawLine.split("\\|")
    Persona(columns(0), columns(1), columns(2))
  }
}

case class Persona(nome: String = "", cogn: String = "", eta: String = "") extends Record {
  override def toList: List[String] = List(nome, cogn, eta)
}
