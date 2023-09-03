package reader

import data.WikiDataQueryResult
import spray.json._
import spray.json.DefaultJsonProtocol._

import java.io.File
import java.time.LocalDate
import scala.io.Source
import scala.util.Try

object WikiDataQueryReader {

  private implicit object LocalDateJsonFormat extends JsonFormat[Option[LocalDate]] {
    def write(currency: Option[LocalDate]): JsValue = throw new UnsupportedOperationException
    def read(value: JsValue): Option[LocalDate] = {
      Try(LocalDate.parse(value.convertTo[String].split("T").head)).toOption
    } // TODO
  }

  private implicit val wikiDataEntryFormat: JsonFormat[WikiDataQueryResult] = jsonFormat8(WikiDataQueryResult)

  def readWikiDataQueryResult(file: File): Seq[WikiDataQueryResult] = {
    val stream = Source.fromFile(file)
    val content = stream.mkString
    stream.close()
    JsonParser(content).convertTo[Seq[WikiDataQueryResult]]
  }

}
