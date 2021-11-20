package reader

import data.WikiDataQueryResult
import spray.json._
import spray.json.DefaultJsonProtocol._

import java.io.File
import java.time.LocalDate
import scala.io.Source

object WikiDataQueryReader {

  private implicit object LocalDateJsonFormat extends JsonFormat[LocalDate] {
    def write(currency: LocalDate): JsValue = throw new UnsupportedOperationException
    def read(value: JsValue): LocalDate = LocalDate.parse(value.convertTo[String].split("T").head) // TODO
  }

  private implicit val wikiDataEntryFormat: JsonFormat[WikiDataQueryResult] = jsonFormat8(WikiDataQueryResult)

  def readWikiDataQueryResult(file: File): Seq[WikiDataQueryResult] = {
    val stream = Source.fromFile(file)
    val content = stream.mkString
    stream.close()
    JsonParser(content).convertTo[Seq[WikiDataQueryResult]]
  }

}
