package reader

import data.PersonBlackListed
import reader.ReaderUtils.csvReader

import java.io.File
import java.time.LocalDate
import java.time.format.DateTimeFormatter

object InseeBlackListReader {

  def readBlackList(file: File): Set[PersonBlackListed] = {
    val formatter = DateTimeFormatter.ofPattern("yyyyMMdd")
    csvReader(file, ';').map(r => PersonBlackListed(LocalDate.parse(r.get(0), formatter), r.get(1), r.get(2))).toSet
  }

}
