package db

import java.io.{File, RandomAccessFile}
import java.util.Date

import data._
import db.file.FileContext
import db.index.{ExactStringMachIndex, ExclusiveSubsetIndex}
import db.result.{DirectPersonResult, DirectPlaceResult, ReferenceResult}
import reader.{InseePersonsReader, InseePlacesReader}

import scala.annotation.tailrec
import scala.collection.Seq

class InseeDatabase(root: File, readonly: Boolean = true) {

  require(root.isDirectory)

  private def relativePath(filename: String): File = new File(root.getAbsolutePath + "/" + filename)

  private def open(filename: String): FileContext = new FileContext(new RandomAccessFile(relativePath(filename), if (readonly) "r" else "rw"))


  /* Files */

  // Persons data (convert a person id to actual data)
  private val personsDataFile: FileContext = open("persons_data.db")

  // Places data (convert a place id to actual data)
  private val placesDataFile: FileContext = open("places_data.db")

  // Surnames and names index (convert a name to an id)
  private val surnamesIndexFile: FileContext = open("surnames_index.db")
  private val namesIndexFile: FileContext = open("names_index.db")

  // Search index (find person ids matching specific constraints)
  private val searchIndexFile: FileContext = open("search_index.db")

  // Places index (convert a string prefix to a list of matching place ids)
  private val placesIndexFile: FileContext = open("places_index.db")


  /* Indices */

  private val personsData = new DirectPersonResult()

  private val placesData = new DirectPlaceResult()

  private val genericNameIndex = new ExactStringMachIndex[String] {
    def getParameter(q: String): scala.collection.Seq[Byte] = q.getBytes
  }

  private val searchIndex = new ExclusiveSubsetIndex[PersonQuery, ResultSet[Int]] {
    override def getParameter(q: PersonQuery): Set[Int] = q.nomsIds.toSet
    override val child: DatabaseLevel[PersonQuery, ResultSet[Int]] =
      new ExclusiveSubsetIndex[PersonQuery, ResultSet[Int]] {
        override def getParameter(q: PersonQuery): Set[Int] = q.prenomsIds.toSet
        override val child: DatabaseLevel[PersonQuery, ResultSet[Int]] = new ReferenceResult()
        // TODO complete this
      }
  }

  //private val placesIndex = ??? // TODO


  /* Query methods */

  private def normalizeString(str: String): String = str.trim.toLowerCase

  private def nameToId(name: String): Option[Int] = genericNameIndex.queryFirst(namesIndexFile, normalizeString(name))

  private def surnameToId(surname: String): Option[Int] = genericNameIndex.queryFirst(surnamesIndexFile, normalizeString(surname))

  private def idToPerson(id: Int): Option[PersonData] = personsData.query(personsDataFile, id, 1, null).entries.headOption

  private def idToPersonDisplay(id: Int): Option[PersonDisplay] = {
    idToPerson(id).map { p =>
      PersonDisplay(p.nom, p.prenom, p.gender, p.birthDate, idToPlaceDisplay(p.birthPlaceId), p.deathDate, idToPlaceDisplay(p.deathPlaceId))
    }
  }

  private def idToPlace(id: Int): Option[PlaceData] = placesData.query(placesDataFile, id, 1, null).entries.headOption

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

  private def idToAbsolutePlace(id: Int): Option[Seq[Int]] = idToPlaces(id).map(_.map(_._1))

  private def idToPlaceDisplay(id: Int): Option[String] = { // TODO change signature
    idToPlaces(id).map(_.map(_._2.name)).map{ places =>
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
  }

  // TODO missing methods

  def queryPersons(limit: Int, offset: Int,
                   surname: Option[String] = None,
                   name: Option[String] = None,
                   placeId: Option[Int] = None,
                   birthAfter: Option[Date] = None, birthBefore: Option[Date] = None,
                   deathAfter: Option[Date] = None, deathBefore: Option[Date] = None): ResultSet[PersonDisplay] = {
    require(limit >= 0 && offset >= 0)

    def processName(name: Option[String], translation: String => Option[Int]): Option[Seq[Int]] = {
      val result = name.map(cleanSplit).getOrElse(Seq.empty).map(translation)
      if(result.exists(_.isEmpty))
        None
      else
        Some(result.flatten)
    }

    val (surnamesOpt, namesOpt) = (processName(surname, surnameToId), processName(name, nameToId))
    val placeOpt = placeId.map(idToAbsolutePlace) match {
      case Some(None) => None
      case other => Some(other.flatten.getOrElse(Seq(0))) // `0` is the root place
    }

    // TODO date parameters

    (surnamesOpt, namesOpt, placeOpt) match {
      case (Some(surnames), Some(names), Some(place)) =>

        val query = PersonQuery(surnames, names, place)

        val result = searchIndex.query(searchIndexFile, offset, limit, query)
        result.copy(entries = result.entries.map(id => idToPersonDisplay(id).get))
      case _ => // A field contains a non existent key
        new ResultSet[PersonDisplay](Seq.empty, 0)
    }
  }

  def queryPlacesByPrefix(limit: Int, prefix: String): Seq[PlaceDisplay] = {

    ???
  }

  private def cleanSplit(str: String): IndexedSeq[String] = str.toLowerCase.trim.split("[^a-z]+").toVector.filter(_.nonEmpty)

  def generateDatabase(inseeFilename: String, inseePlaceDirectory: String): Unit = {
    import scala.collection._

    val placeTree = InseePlacesReader.readPlaces(relativePath(inseePlaceDirectory))

    var idCounter = 0
    def processPlaceTree(node: PlaceTree, parent: Option[Int]): (Map[Int, PlaceData], Map[String, Int]) = {
      val id = idCounter
      idCounter += 1
      val placeData = PlaceData(node.name, parent)
      node.children.foldLeft((Map(id -> placeData), node.inseeCodeOpt.map(code => Map(code -> id)).getOrElse(Map.empty))) {
        case ((left, right), child) =>
          val (cl, cr) = processPlaceTree(child, Some(id))
          ((cl ++ left).toMap, (cr ++ right).toMap)
      }
    }

    val (idPlaceMap, inseeCodePlaceMap) = processPlaceTree(placeTree, None)

    placesData.write(placesDataFile, idPlaceMap) // Write place data


    val iterator = InseePersonsReader.readCompiledFile(relativePath(inseeFilename)).filter(InseePersonsReader.isReasonable)
      .take(10000) // TODO temporary

    val (nomsSet, prenomsSet) = (mutable.HashSet.empty[String], mutable.HashSet.empty[String])
    val personsDataMap = mutable.Map.empty[Int, PersonData]
    var count = 0
    iterator.foreach { p =>
      val id = count
      nomsSet.add(p.nom)
      prenomsSet.add(p.prenom)
      personsDataMap.put(id, PersonData(p.nom, p.prenom, p.gender, p.birthDate, inseeCodePlaceMap.getOrElse(p.birthCode, 0), p.deathDate, inseeCodePlaceMap.getOrElse(p.deathCode, 0)))

      count += 1
    }

    val (nomsMap, prenomsMap) = (nomsSet.toSeq.sorted.zipWithIndex.map(_.swap).toMap, prenomsSet.toSeq.sorted.zipWithIndex.map(_.swap).toMap)

    genericNameIndex.write(surnamesIndexFile, nomsMap)
    genericNameIndex.write(surnamesIndexFile, prenomsMap)
    personsData.write(personsDataFile, personsDataMap)

    // TODO write search

    ???
  }


  def dispose(): Unit = {
    personsDataFile.close()
    placesDataFile.close()
    surnamesIndexFile.close()
    namesIndexFile.close()
    searchIndexFile.close()
    placesIndexFile.close()
  }
}
