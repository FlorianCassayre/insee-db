package reader

import java.io.{BufferedReader, File, FileInputStream, InputStreamReader}
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.zip.GZIPInputStream

import data.PersonRaw
import db.util.StringUtils
import org.apache.commons.csv.CSVFormat

import scala.jdk.CollectionConverters._
import scala.util.Try

object InseePersonsReader {

  def readOfficialYearlyFile(file: File): Iterable[PersonRaw] = {
    val iterator = CSVFormat.DEFAULT.withFirstRecordAsHeader().withDelimiter(';').parse(new BufferedReader(new InputStreamReader(new FileInputStream(file))))

    val formatter = DateTimeFormatter.ofPattern("yyyyMMdd")

    iterator.asScala.view.map { r =>
      val namesEntry = r.get(0)
      val split = namesEntry.split("\\*")
      val (surname, nameSlash) = split match {
        case Array(a, b) => (a, b)
        case Array(a) => (a, "")
      }
      val endsWithSlash = nameSlash.nonEmpty && nameSlash.last == '/'
      val name = if(endsWithSlash) nameSlash.substring(0, nameSlash.length - 1) else nameSlash

      PersonRaw(
        surname.toUpperCase,
        StringUtils.capitalizeFirstPerWord(name),
        parseGender(r.get(1)),
        parseDate(formatter, r.get(2)),
        r.get(3),
        parseDate(formatter, r.get(6)),
        r.get(7),
        r.get(8)
      )
    }
  }

  private def parseGender(str: String): Boolean = str match {
    case "1" => true
    case "2" => false
  }

  private def parseDate(formatter: DateTimeFormatter, str: String): Option[LocalDate] = Try(LocalDate.parse(str, formatter)).toOption

  /**
   * A rough filter to detect outliers.
   * On the current dataset it removes 80 incoherent entries.
   * @param person the person to inspect
   * @return `false` if the entry is considered to be an outlier
   */
  def isReasonable(person: PersonRaw): Boolean = {
    val AgeMax = 125
    val YearBirthMin = 1850
    val YearBirthMax = 2022
    val YearDeathMax = 2022

    def isReasonableName(string: String): Boolean = string.forall(!_.isDigit)
    val isReasonableRange = (person.birthDate, person.deathDate) match {
      case (Some(birth), Some(death)) =>
        val isRange = birth.equals(death) || death.isAfter(birth)
        val difference = death.getYear - birth.getYear <= AgeMax
        isRange && difference
      case _ => true
    }
    val isReasonableBirth = person.birthDate.map(_.getYear).forall(y => YearBirthMin <= y && y <= YearBirthMax)
    val isReasonableDeath = person.deathDate.map(_.getYear).forall(y => y <= YearDeathMax)
    isReasonableRange && isReasonableBirth && isReasonableDeath && isReasonableName(person.nom) && isReasonableName(person.prenom)
  }

}
