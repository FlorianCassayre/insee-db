package reader

import java.io.{BufferedReader, File, FileInputStream, InputStreamReader}

import org.apache.commons.csv.{CSVFormat, CSVRecord}

import scala.jdk.CollectionConverters._

object ReaderUtils {

  def csvReader(file: File, delimiter: Char = ','): Iterable[CSVRecord] = {
    CSVFormat.DEFAULT.withFirstRecordAsHeader().withDelimiter(delimiter).parse(new BufferedReader(new InputStreamReader(new FileInputStream(file), "UTF-8"))).asScala
  }

}
