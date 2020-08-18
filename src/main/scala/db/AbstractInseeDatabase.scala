package db

import java.io.File

import data.{AbstractNamePlaceQuery, PersonProcessed, PersonQuery, PlaceData}
import db.file.writer.{DataHandler, ShortHandler, ThreeBytesHandler}
import db.index.{ExactStringMachIndex, ExclusiveSubsetIndex, PointerBasedIndex, PrefixIndex, StringBasedIndex}
import db.result.{DirectDateResult, DirectPersonResult, DirectPlaceResult, LimitedReferenceResult, ReferenceResult}
import db.util.DateUtils

import scala.collection.Seq

abstract class AbstractInseeDatabase(root: File) {

  require(root.exists, "Root directory doesn't exist")
  require(root.isDirectory, "Root path is not a directory")

  protected def relative(filename: String): File = new File(root.getAbsolutePath + File.separator + filename)

  protected val (
    personsDataFile, // Persons data (convert a person id to actual data)
    placesDataFile, // Places data (convert a place id to actual data)
    surnamesIndexFile, // Surnames index (convert a name to an id)
    namesIndexFile, // Identical to surnames, but for names
    searchIndexFile, // Search index (find person ids matching specific constraints)
    placesIndexFile, // Places index (convert a string prefix to a list of matching place ids)
    datesDataFile // Dates represented as years for fast lookup
    ) = (
    relative("persons_data.db"),
    relative("places_data.db"),
    relative("surnames_index.db"),
    relative("names_index.db"),
    relative("search_index.db"),
    relative("places_index.db"),
    relative("dates_data.db")
  )

  /* Indices */

  protected val personsData = new DirectPersonResult()

  protected val placesData = new DirectPlaceResult()

  protected val genericNameIndex: ExactStringMachIndex[String, String] = new ExactStringMachIndex[String, String] {
    override def getQueryParameter(q: String): Seq[Byte] = q.getBytes
    override def getWriteParameter(q: String): Seq[Byte] = getQueryParameter(q)
  }

  protected abstract class SurnameSetLevel[Q <: AbstractNamePlaceQuery, R] extends ExclusiveSubsetIndex[Q, PersonProcessed, R] with PointerBasedIndex {
    override val ignoreRoot: Boolean = true
    override val keyHandler: DataHandler = new ThreeBytesHandler()
    override def getQueryParameter(q: Q): Seq[Int] = q.nomsIds
    override def getWriteParameter(q: PersonProcessed): Seq[Int] = q.noms
  }

  protected abstract class GivenNameSetLevel[Q <: AbstractNamePlaceQuery, R] extends ExclusiveSubsetIndex[Q, PersonProcessed, R] with PointerBasedIndex {
    override val keyHandler: DataHandler = new ThreeBytesHandler()
    override def getQueryParameter(q: Q): Seq[Int] = q.prenomsIds
    override def getWriteParameter(q: PersonProcessed): Seq[Int] = q.prenoms
  }

  protected abstract class PlaceTreeLevel[Q <: AbstractNamePlaceQuery, R] extends PrefixIndex[Q, PersonProcessed, R] with PointerBasedIndex {
    override val keyHandler: DataHandler = new ShortHandler()
    override def getQueryParameter(q: Q): Seq[Seq[Int]] = Seq(q.placeIds)
    override def getWriteParameter(q: PersonProcessed): Seq[Seq[Int]] = Seq(q.birthPlaceIds, q.deathPlaceIds).toSet.toSeq
  }

  protected class DualDateSortedPersonResult extends ReferenceResult[PersonQuery, PersonProcessed] {
    override val OrdersCount: Int = 2
    override def ordering(i: Int)(id: Int, value: PersonProcessed): Long = i match {
      case 0 => value.birthDate.map(DateUtils.toMillis).getOrElse(Long.MaxValue) // Birth date
      case 1 => value.deathDate.map(DateUtils.toMillis).getOrElse(Long.MaxValue) // Death date
    }
    override def getOrder(q: PersonQuery): Int = if(q.filterByBirth) 0 else 1
    override def orderTransformer(i: Int)(id: Int): Int = idToDate(id, i).get
    override def lowerBound(i: Int)(value: PersonQuery): Option[Int] = value.yearMin // i is unused here
    override def upperBound(i: Int)(value: PersonQuery): Option[Int] = value.yearMax
    override def isAscending(i: Int)(value: PersonQuery): Boolean = value.ascending
  }

  type PersonLevel = DatabaseLevel[PersonQuery, PersonProcessed, ResultSet[Int]]

  protected val searchIndex: PersonLevel = new SurnameSetLevel[PersonQuery, ResultSet[Int]] {
    override val child: PersonLevel = new GivenNameSetLevel[PersonQuery, ResultSet[Int]] {
      override val child: PersonLevel = new PlaceTreeLevel[PersonQuery, ResultSet[Int]] {
        override val child: PersonLevel = new DualDateSortedPersonResult
      }
    }
  }

  type PlaceLevel = DatabaseLevel[String, (String, Int), ResultSet[Int]]

  protected val placesIndex: PlaceLevel = new PrefixIndex[String, (String, Int), ResultSet[Int]] with StringBasedIndex {
    override def getQueryParameter(q: String): Seq[Seq[Int]] = Seq(q.getBytes.map(_.toInt).toSeq)
    override def getWriteParameter(q: (String, Int)): Seq[Seq[Int]] = getQueryParameter(q._1)
    override val child: PlaceLevel = new LimitedReferenceResult[String, (String, Int)]() {
      override val MaxResults: Int = 25
      override def ordering(i: Int)(id: Int, p: (String, Int)): Long = i match {
        case 0 => -p._2
      }
    }
  }

  protected val datesData: DirectDateResult = new DirectDateResult()

  protected def idToDate(id: Int, kind: Int): Option[Int]

  protected def placeDisplay(absolute: Seq[PlaceData]): String = {
    val places = absolute.map(_.name).tail.reverse
    def join(seq: Seq[String]): String = seq.mkString(", ")
    if(places.size >= 5) {
      val (prefix, suffix) = places.splitAt(places.size - 4)
      val prefixParenthesis = s"(${join(prefix)})"
      val suffixJoined = join(suffix)
      s"$prefixParenthesis $suffixJoined"
    } else {
      join(places)
    }
  }

  private[db] val BaseYear: Int = datesData.BaseYear

  private[db] val RootPlaceId: Int = 0

}
