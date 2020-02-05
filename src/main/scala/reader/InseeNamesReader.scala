package reader

import java.io.File

import db.util.StringUtils
import reader.ReaderUtils._

/**
 * https://www.data.gouv.fr/fr/datasets/ficher-des-prenoms-de-1900-a-2018/
 */
object InseeNamesReader {

  def readNames(file: File): Map[String, String] = {
    csvReader(file, ';').map(r => (StringUtils.capitalizeFirstPerWord(r.get(1)), r.get(3).toInt)).groupMapReduce(_._1)(_._2)(_ + _)
      .groupBy(t => StringUtils.unaccent(t._1.trim.toLowerCase)).view.mapValues(_.toIndexedSeq.maxBy(_._2)._1).toMap
  }

}
