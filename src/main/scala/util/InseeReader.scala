package util

import java.io.{BufferedReader, File, FileInputStream, InputStreamReader}
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}
import java.util.zip.GZIPInputStream

import data.Person
import org.apache.commons.csv.CSVFormat

import scala.jdk.CollectionConverters._
import scala.util.Try

object InseeReader {

  private val calendar = Calendar.getInstance()

  def readCompiledFile(file: File): Iterable[Person] = {

    val buffered = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(file)), "UTF-8"))

    val iterator = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(buffered)

    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")

    def parseGender(str: String): Boolean = str match {
      case "1" => true
      case "2" => false
    }

    def parseDate(str: String): Option[Date] = Try(dateFormat.parse(str)).toOption

    // `view` is required to preserve the iterator
    iterator.asScala.view.map { r =>
      Person(r.get(0), r.get(1), parseGender(r.get(2)), parseDate(r.get(3)), r.get(4), parseDate(r.get(7)), r.get(8))
    }
  }

  /**
   * A rough filter to detect outliers.
   * On the current dataset it removes 80 incoherent entries.
   * @param person the person to inspect
   * @return `false` if the entry is considered to be an outlier
   */
  def isReasonable(person: Person): Boolean = {
    val AgeMax = 125
    val YearBirthMin = 1850
    val YearBirthMax = 2019
    val YearDeathMax = 2019

    def isReasonableName(string: String): Boolean = string.forall(!_.isDigit)
    def getYear(date: Date): Int = {
      calendar.setTime(date)
      calendar.get(Calendar.YEAR)
    }
    val isReasonableRange = (person.birthDate, person.deathDate) match {
      case (Some(birth), Some(death)) =>
        val isRange = birth.equals(death) || death.after(birth)
        val difference = getYear(death) - getYear(birth) <= AgeMax
        isRange && difference
      case _ => true
    }
    val isReasonableBirth = person.birthDate.map(getYear).forall(y => YearBirthMin <= y && y <= YearBirthMax)
    val isReasonableDeath = person.deathDate.map(getYear).forall(y => y <= YearDeathMax)
    isReasonableRange && isReasonableBirth && isReasonableDeath && isReasonableName(person.nom) && isReasonableName(person.prenom)
  }


}
