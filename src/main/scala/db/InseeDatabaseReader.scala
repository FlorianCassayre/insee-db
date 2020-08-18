package db

import java.io.{File, RandomAccessFile}

import data._
import db.file.FileContextIn
import db.index.{PrefixIndex, PrefixIndexStats}
import db.util.StringUtils._
import static.Geography
import static.Geography.StaticPlace

import scala.annotation.tailrec
import scala.collection.{Map, Seq, Set}

class InseeDatabaseReader(root: File) extends AbstractInseeDatabase(root) {

  private def open(file: File): FileContextIn = new FileContextIn(new RandomAccessFile(file, "r"))

  /* Read-only files */

  private val (
    personsDataFileIn,
    placesDataFileIn,
    surnamesIndexFileIn,
    namesIndexFileIn,
    searchIndexFileIn,
    placesIndexFileIn,
    datesDataFileIn
    ) = (
    open(personsDataFile),
    open(placesDataFile),
    open(surnamesIndexFile),
    open(namesIndexFile),
    open(searchIndexFile),
    open(placesIndexFile),
    open(datesDataFile)
  )

  /* Additional views */

  class PlaceStatsResult extends PlaceTreeLevel[PlaceStatisticsQuery, Map[Int, Int]] with PrefixIndexStats[PlaceStatisticsQuery, PersonProcessed, Seq[Seq[Int]]]

  type StatsLevel = DatabaseLevel[PlaceStatisticsQuery, PersonProcessed, Map[Int, Int]]

  protected val searchStatsIndex: StatsLevel = new SurnameSetLevel[PlaceStatisticsQuery, Map[Int, Int]] {
    override val child: StatsLevel = new GivenNameSetLevel[PlaceStatisticsQuery, Map[Int, Int]] {
      override val child: StatsLevel = new PlaceStatsResult
    }
  }

  /* Query methods */

  def nameToId(name: String): Option[Int] = genericNameIndex.queryFirst(namesIndexFileIn, normalizeString(name))

  def surnameToId(surname: String): Option[Int] = genericNameIndex.queryFirst(surnamesIndexFileIn, normalizeString(surname))

  private def idToPerson(id: Int): Option[PersonData] = personsData.query(personsDataFileIn, id, 1, null).entries.headOption

  def idToPersonDisplay(id: Int): Option[PersonDisplay] = {
    idToPerson(id).map { p =>
      PersonDisplay(p.nom, p.prenom, p.gender, p.birthDate, idToPlaceDisplay(p.birthPlaceId), p.deathDate, idToPlaceDisplay(p.deathPlaceId))
    }
  }

  private def idToPlace(id: Int): Option[PlaceData] = placesData.query(placesDataFileIn, id, 1, null).entries.headOption

  private def idToPlaces(id: Int): Option[Seq[(Int, PlaceData)]] = {
    idToPlace(id).map { place =>
      @tailrec
      def upTraversal(id: Int, node: PlaceData, acc: Seq[(Int, PlaceData)]): Seq[(Int, PlaceData)] = {
        val newAcc = (id, node) +: acc
        node.parent match {
          case Some(parentId) =>
            val parent = idToPlace(parentId).get // Assume parent exists
            upTraversal(parentId, parent, newAcc)
          case None =>
            newAcc
        }
      }
      upTraversal(id, place, Seq.empty)
    }
  }

  def idToAbsolutePlace(id: Int): Option[Seq[Int]] = idToPlaces(id).map(_.map(_._1))

  def idToPlaceDisplay(id: Int): Option[String] = {
    idToPlaces(id).map(d => placeDisplay(d.map(_._2)))
  }

  override protected def idToDate(id: Int, kind: Int): Option[Int] = datesData.query(datesDataFileIn, 0, 1, (kind, id))

  def queryPersonsId(offset: Int, limit: Int, parameters: PersonQuery): ResultSet[Int] = searchIndex.query(searchIndexFileIn, offset, limit, parameters)

  private def namesToIds(surname: Option[String] = None, name: Option[String] = None): Option[(Seq[Int], Seq[Int])] = {
    def processName(name: Option[String], translation: String => Option[Int]): Option[Seq[Int]] = {
      val result = name.map(cleanSplitAndNormalize).getOrElse(Seq.empty).map(translation)
      if(result.exists(_.isEmpty))
        None
      else
        Some(result.flatten)
    }

    val (surnamesOpt, namesOpt) = (processName(surname, surnameToId), processName(name, nameToId))

    (surnamesOpt, namesOpt) match {
      case (Some(surnames), Some(names)) => Some((surnames, names))
      case _ => None // A field contains a non existent key
    }
  }

  def queryPersons(offset: Int, limit: Int,
                   surname: Option[String] = None,
                   name: Option[String] = None,
                   placeId: Option[Int] = None,
                   filterByBirth: Boolean = true,
                   after: Option[Int] = None, before: Option[Int] = None,
                   ascending: Boolean = true
                  ): ResultSet[PersonDisplay] = {
    require(limit >= 0 && offset >= 0)

    val namesOpt = namesToIds(surname, name)

    val placeOpt = placeId.map(idToAbsolutePlace) match {
      case Some(None) => None
      case other => Some(other.flatten.getOrElse(Seq(RootPlaceId)))
    }

    (namesOpt, placeOpt) match {
      case (Some((surnames, names)), Some(place)) =>

        val query = PersonQuery(surnames, names, place, filterByBirth = filterByBirth, ascending = ascending, after.map(_ - BaseYear), before.map(_ - BaseYear))

        val result = searchIndex.query(searchIndexFileIn, offset, limit, query)

        result.copy(entries = result.entries.map(id => idToPersonDisplay(id).get))
      case _ =>
        new ResultSet[PersonDisplay](Seq.empty, 0)
    }
  }

  def queryPlacesByPrefix(limit: Int, prefix: String): Seq[PlaceDisplay] = {
    val normalized = normalizeSentence(prefix)
    placesIndex.query(placesIndexFileIn, 0, limit, normalized).entries.map(id => PlaceDisplay(id, idToPlaceDisplay(id).get))
  }

  private[db] def queryPlaceStatisticsId(surname: Option[String] = None, name: Option[String] = None, placeIds: Seq[Int]): Map[Int, Int] = {
    namesToIds(surname, name) match {
      case Some((surnames, names)) =>
        searchStatsIndex.query(searchIndexFileIn, 0, Int.MaxValue, PlaceStatisticsQuery(surnames, names, placeIds))
      case None => Map.empty
    }
  }

  /* Data to be loaded */

  protected val (placeCodeToPlaceId, placeIdToPlaceCode) = {
    def explore(staticPlace: StaticPlace, suffix: Seq[String] = Seq.empty): Map[String, String] = {
      val newSuffix = staticPlace.name +: suffix
      val entry = staticPlace.code -> newSuffix.mkString(", ")
      staticPlace.children.map(child => explore(child, newSuffix)).fold(Map.empty)(_ ++ _) + entry
    }
    // code -> name
    val places = Geography.StaticPlaces.map(place => explore(place)).fold(Map.empty)(_ ++ _)
    val pairs = places.view.mapValues { name =>
      // TODO: update the search index to provide exact result first
      val Some(result) = queryPlacesByPrefix(10, name).find(_.fullname == name)
      result.id
    }.toSeq

    (pairs.toMap, pairs.map(_.swap).toMap)
  }

  /* Methods that depend on the above data */

  def queryPlaceStatisticsCode(surname: Option[String] = None, name: Option[String] = None, placeCode: Option[String]): Map[String, Int] = {
    val result = placeCode match {
      case None => // Root
        queryPlaceStatisticsId(surname, name, Seq())
      case Some(code) => // Other
        placeCodeToPlaceId.get(code).map(id =>
          queryPlaceStatisticsId(surname, name, idToAbsolutePlace(id).get)
        ).getOrElse(Map.empty)
    }
    result.map { case (k, v) => placeIdToPlaceCode(k) -> v }
  }

  /* Finalize */

  def dispose(): Unit = {
    personsDataFileIn.close()
    placesDataFileIn.close()
    surnamesIndexFileIn.close()
    namesIndexFileIn.close()
    searchIndexFileIn.close()
    placesIndexFileIn.close()
    datesDataFileIn.close()
  }
}
