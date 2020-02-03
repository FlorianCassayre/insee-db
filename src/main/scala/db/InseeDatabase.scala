package db

import java.io.{BufferedOutputStream, DataOutputStream, File, FileOutputStream, RandomAccessFile}
import java.util.Date

import data._
import db.file.FileContext
import db.file.writer.DataHandler
import db.index.{ExactStringMachIndex, ExclusiveSubsetIndex, PointerBasedIndex, PrefixIndex, StringBasedIndex}
import db.result.{DirectPersonResult, DirectPlaceResult, LimitedReferenceResult, ReferenceResult}
import reader.{InseePersonsReader, InseePlacesReader}

import scala.annotation.tailrec
import scala.collection.{Seq, Set}

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

  private val genericNameIndex = new ExactStringMachIndex[String, String] {
    override def getQueryParameter(q: String): Seq[Byte] = q.getBytes
    override def getWriteParameter(q: String): Seq[Byte] = getQueryParameter(q)
  }

  private val searchIndex: DatabaseLevel[PersonQuery, PersonProcessed, ResultSet[Int]] = new ExclusiveSubsetIndex[PersonQuery, PersonProcessed, ResultSet[Int]] with PointerBasedIndex {
    override val ignoreRoot: Boolean = true
    override def getQueryParameter(q: PersonQuery): Set[Int] = q.nomsIds.toSet
    override def getWriteParameter(q: PersonProcessed): Set[Int] = q.noms.toSet
    override val child: DatabaseLevel[PersonQuery, PersonProcessed, ResultSet[Int]] =
      new ExclusiveSubsetIndex[PersonQuery, PersonProcessed, ResultSet[Int]] with PointerBasedIndex {
        override def getQueryParameter(q: PersonQuery): Set[Int] = q.prenomsIds.toSet
        override def getWriteParameter(q: PersonProcessed): Set[Int] = q.prenoms.toSet
        override val child: DatabaseLevel[PersonQuery, PersonProcessed, ResultSet[Int]] = new PrefixIndex[PersonQuery, PersonProcessed, ResultSet[Int]] with PointerBasedIndex {
          override def getQueryParameter(q: PersonQuery): Seq[Seq[Int]] = Seq(q.placeIds)
          override def getWriteParameter(q: PersonProcessed): Seq[Seq[Int]] = Seq(q.birthPlaceIds, q.deathPlaceIds).toSet.toSeq
          override val child: DatabaseLevel[PersonQuery, PersonProcessed, ResultSet[Int]] = new ReferenceResult()
          // TODO complete this
        }
      }
  }

  private val placesIndex: DatabaseLevel[String, (String, Int), ResultSet[Int]] = new PrefixIndex[String, (String, Int), ResultSet[Int]] with StringBasedIndex {
    override def getQueryParameter(q: String): Seq[Seq[Int]] = Seq(q.getBytes.map(_.toInt).toSeq)
    override def getWriteParameter(q: (String, Int)): Seq[Seq[Int]] = getQueryParameter(q._1)
    override val child: DatabaseLevel[String, (String, Int), ResultSet[Int]] = new LimitedReferenceResult[String, (String, Int)]() {
      override val MaxResults: Int = 25
      override def ordering(id: Int, p: (String, Int)): Int = -p._2
    }
  }

  /* Query methods */

  private def nameToId(name: String): Option[Int] = genericNameIndex.queryFirst(namesIndexFile, normalizeString(name))

  private def surnameToId(surname: String): Option[Int] = genericNameIndex.queryFirst(surnamesIndexFile, normalizeString(surname))

  private def idToPerson(id: Int): Option[PersonData] = personsData.query(personsDataFile, id, 1, null).entries.headOption

  def idToPersonDisplay(id: Int): Option[PersonDisplay] = {
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
    idToPlaces(id).map(d => placeDisplay(d.map(_._2)))
  }

  private def placeDisplay(absolute: Seq[PlaceData]): String = {
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

  // TODO missing methods

  def queryPersons(offset: Int, limit: Int,
                   surname: Option[String] = None,
                   name: Option[String] = None,
                   placeId: Option[Int] = None,
                   birthAfter: Option[Date] = None, birthBefore: Option[Date] = None,
                   deathAfter: Option[Date] = None, deathBefore: Option[Date] = None): ResultSet[PersonDisplay] = {
    require(limit >= 0 && offset >= 0)

    def processName(name: Option[String], translation: String => Option[Int]): Option[Seq[Int]] = {
      val result = name.map(cleanSplitAndNormalize).getOrElse(Seq.empty).map(translation)
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
    val normalized = normalizeSentence(prefix)
    placesIndex.query(placesIndexFile, 0, limit, normalized).entries.map(id => PlaceDisplay(id, idToPlaceDisplay(id).get))
  }

  private def cleanSplit(str: String): IndexedSeq[String] = str.trim.split("[^a-z]+").toVector.filter(_.nonEmpty)

  private def normalizeString(str: String): String = {
    import java.text.Normalizer
    Normalizer.normalize(str.trim.toLowerCase, Normalizer.Form.NFD).replaceAll("\\p{InCombiningDiacriticalMarks}+", "")
  } // TODO

  private def normalizeSentence(str: String): String = cleanSplitAndNormalize(str).mkString(" ")

  private def cleanSplitAndNormalize(str: String): IndexedSeq[String] = cleanSplit(normalizeString(str))

  def generateDatabase(inseeFile: File, inseePlaceDirectory: File): Unit = {
    import scala.collection._

    var placeTree = InseePlacesReader.readPlaces(inseePlaceDirectory)

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

    var (idPlaceMap, inseeCodePlaceMap) = processPlaceTree(placeTree, None)

    placeTree = null

    def placeAbsolute(id: Int): Seq[Int] = {
      @tailrec
      def upTraversal(id: Int, place: PlaceData, acc: Seq[Int]): Seq[Int] = {
        val newAcc = id +: acc
        place.parent match {
          case Some(parentId) => upTraversal(parentId, idPlaceMap(parentId), newAcc)
          case None => newAcc
        }
      }
      upTraversal(id, idPlaceMap(id), Seq.empty)
    }

    placesData.write(placesDataFile, idPlaceMap) // Write place data


    val iterator = InseePersonsReader.readCompiledFile(inseeFile).filter(InseePersonsReader.isReasonable)
      .take(1000) // TODO temporary

    var (nomsSet, prenomsSet) = (mutable.HashSet.empty[String], mutable.HashSet.empty[String])
    var personsDataMap = mutable.Map.empty[Int, PersonData]
    var placeOccurrences = mutable.Seq.fill(idPlaceMap.size)(0)
    var count = 0
    iterator.foreach { p =>
      val id = count
      nomsSet.addAll(cleanSplitAndNormalize(p.nom))
      prenomsSet.addAll(cleanSplitAndNormalize(p.prenom))
      val (birthPlace, deathPlace) = (inseeCodePlaceMap.getOrElse(p.birthCode, 0), inseeCodePlaceMap.getOrElse(p.deathCode, 0))
      personsDataMap.put(id, PersonData(p.nom, p.prenom, p.gender, p.birthDate, birthPlace, p.deathDate, deathPlace))

      Seq(birthPlace, deathPlace).flatMap(placeAbsolute).foreach { id =>
        placeOccurrences.update(id, placeOccurrences(id) + 1)
      }

      count += 1
    }

    inseeCodePlaceMap = null

    placesIndex.write(placesIndexFile, idPlaceMap.keys.map(id => id -> (normalizeSentence(placeDisplay(placeAbsolute(id).map(idPlaceMap))), placeOccurrences(id))).toMap)

    placeOccurrences = null

    var (nomsSorted, prenomsSorted) = (nomsSet.toSeq.sorted, prenomsSet.toSeq.sorted)

    nomsSet = null
    prenomsSet = null

    var (nomsMap, prenomsMap) = (nomsSorted.zipWithIndex.toMap, prenomsSorted.zipWithIndex.toMap)

    genericNameIndex.write(surnamesIndexFile, nomsSorted.zipWithIndex.map(_.swap).toMap)
    genericNameIndex.write(namesIndexFile, prenomsSorted.zipWithIndex.map(_.swap).toMap)

    nomsSorted = null
    prenomsSorted = null

    personsData.write(personsDataFile, personsDataMap)

    val searchValues = personsDataMap.view.mapValues(p => PersonProcessed(cleanSplitAndNormalize(p.nom).map(nomsMap), cleanSplitAndNormalize(p.prenom).map(prenomsMap), p.gender, p.birthDate, placeAbsolute(p.birthPlaceId), p.deathDate, placeAbsolute(p.deathPlaceId))).toMap

    personsDataMap = null
    nomsMap = null
    prenomsMap = null
    idPlaceMap = null

    searchIndex.write(searchIndexFile, searchValues)

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
