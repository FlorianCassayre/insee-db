package util

import java.io.{BufferedReader, File, FileInputStream, InputStreamReader}
import java.util.Date
import java.util.zip.GZIPInputStream

import data.Person
import org.apache.commons.csv.CSVFormat

import scala.jdk.CollectionConverters._

object InseeReader {

  def readCompiledFile(file: File): Iterable[Person] = {

    val buffered = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(file)), "UTF-8"))

    val iterator = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(buffered)

    def parseGender(str: String): Boolean = str match {
      case "1" => true
      case "2" => false
    }

    def parseDate(str: String): Option[Date] = {
      ???
    }

    iterator.asScala.map { r =>
      Person(r.get(0), r.get(1), parseGender(r.get(2)), parseDate(r.get(3)), r.get(4), parseDate(r.get(7)), r.get(8))
    }
  }

}
