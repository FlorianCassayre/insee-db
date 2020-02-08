package db

import java.io.{File, RandomAccessFile}
import java.util.Date

import data._
import db.file.{FileContextIn, FileContextOut}
import db.index._
import db.result.{DirectPersonResult, DirectPlaceResult, LimitedReferenceResult, ReferenceResult}
import db.util.StringUtils
import db.util.StringUtils._
import reader.{InseeNamesReader, InseePersonsReader, InseePlacesReader}

import scala.annotation.tailrec
import scala.collection.{Map, Seq}

class InseeDatabase(root: File, readonly: Boolean = true) {

  require(root.isDirectory)

  private def relative(filename: String): File = new File(root.getAbsolutePath + "/" + filename)

  private def open(file: File): FileContextIn = new FileContextIn(new RandomAccessFile(file, if (readonly) "r" else "rw"))


  /* Files */

  val (personsDataFile, placesDataFile, surnamesIndexFile, namesIndexFile, searchIndexFile, placesIndexFile) =
    (relative("persons_data.db"), relative("places_data.db"), relative("surnames_index.db"), relative("names_index.db"), relative("search_index.db"), relative("places_index.db"))

  // Persons data (convert a person id to actual data)
  private val personsDataFileIn: FileContextIn = open(personsDataFile)

  // Places data (convert a place id to actual data)
  private val placesDataFileIn: FileContextIn = open(placesDataFile)

  // Surnames and names index (convert a name to an id)
  private val surnamesIndexFileIn: FileContextIn = open(surnamesIndexFile)
  private val namesIndexFileIn: FileContextIn = open(namesIndexFile)

  // Search index (find person ids matching specific constraints)
  private val searchIndexFileIn: FileContextIn = open(searchIndexFile)

  // Places index (convert a string prefix to a list of matching place ids)
  private val placesIndexFileIn: FileContextIn = open(placesIndexFile)

  /* Indices */

  private val personsData = new DirectPersonResult()

  private val placesData = new DirectPlaceResult()

  private val genericNameIndex = new ExactStringMachIndex[String, String] {
    override def getQueryParameter(q: String): Seq[Byte] = q.getBytes
    override def getWriteParameter(q: String): Seq[Byte] = getQueryParameter(q)
  }

  private val searchIndex: DatabaseLevel[PersonQuery, PersonProcessed, ResultSet[Int]] = new ExclusiveSubsetIndex[PersonQuery, PersonProcessed, ResultSet[Int]] with PointerBasedIndex {
    override val ignoreRoot: Boolean = true
    override def getQueryParameter(q: PersonQuery): Seq[Int] = q.nomsIds
    override def getWriteParameter(q: PersonProcessed): Seq[Int] = q.noms
    override val child: DatabaseLevel[PersonQuery, PersonProcessed, ResultSet[Int]] =
      new ExclusiveSubsetIndex[PersonQuery, PersonProcessed, ResultSet[Int]] with PointerBasedIndex {
        override def getQueryParameter(q: PersonQuery): Seq[Int] = q.prenomsIds
        override def getWriteParameter(q: PersonProcessed): Seq[Int] = q.prenoms
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

  private def nameToId(name: String): Option[Int] = genericNameIndex.queryFirst(namesIndexFileIn, normalizeString(name))

  private def surnameToId(surname: String): Option[Int] = genericNameIndex.queryFirst(surnamesIndexFileIn, normalizeString(surname))

   def idToPerson(id: Int): Option[PersonData] = personsData.query(personsDataFileIn, id, 1, null).entries.headOption

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

        val result = searchIndex.query(searchIndexFileIn, offset, limit, query)
        result.copy(entries = result.entries.map(id => idToPersonDisplay(id).get))
      case _ => // A field contains a non existent key
        new ResultSet[PersonDisplay](Seq.empty, 0)
    }
  }

  def queryPlacesByPrefix(limit: Int, prefix: String): Seq[PlaceDisplay] = {
    val normalized = normalizeSentence(prefix)
    placesIndex.query(placesIndexFileIn, 0, limit, normalized).entries.map(id => PlaceDisplay(id, idToPlaceDisplay(id).get))
  }

  private def write[P](level: DatabaseLevel[_, P, _], data: Map[Int, P], file: File): Unit = {
    val context = new FileContextOut(file)
    level.write(context, data)
    print("Flushing... ")
    context.close()
  }

  def generateDatabase(inseeCompiledFile: File, inseePlaceDirectory: File, inseeNamesFiles: File): Unit = {
    import scala.collection._

    val t0 = System.currentTimeMillis()
    def getLogPrefix(): String = "[" + (System.currentTimeMillis() - t0).toString.reverse.padTo(10, '0').reverse + "]: "
    def log(message: String): Unit = println(getLogPrefix() + message)
    def logEllipse(message: String): Unit = print(getLogPrefix() + message + "... ")
    def logOK(): Unit = println("OK")

    log("Generating database")

    logEllipse("Loading places")

    var (placeTree, countryTranslation) = InseePlacesReader.readPlaces(inseePlaceDirectory) // TODO

    logOK()

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

    logEllipse("Building place tree")

    var (idPlaceMap, inseeCodePlaceMap) = processPlaceTree(placeTree, None)

    logOK()

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

    logEllipse("Writing places data")

    write(placesData, idPlaceMap, placesDataFile) // Write place data

    logOK()

    logEllipse("Reading formatted names")

    var namesAccent = InseeNamesReader.readNames(inseeNamesFiles)

    logOK()

    logEllipse("Iterating through dataset")

    val iterator = InseePersonsReader.readCompiledFile(inseeCompiledFile).filter(InseePersonsReader.isReasonable)
      .take(1000) // TODO temporary

    val stopRegex = "[^a-z]+".r
    var (nomsSet, prenomsSet) = (mutable.HashSet.empty[String], mutable.HashSet.empty[String])
    var personsDataMap = mutable.Map.empty[Int, PersonData]
    var placeOccurrences = mutable.Seq.fill(idPlaceMap.size)(0)
    var count = 0
    iterator.foreach { p =>
      val id = count
      val prenomNormal = normalizeString(p.prenom)
      val stop = stopRegex.findAllIn(prenomNormal).toSeq
      val split = stopRegex.split(prenomNormal)
      val prenomPretty =
        if(stop.size == split.length - 1)
          split.map(s => namesAccent.getOrElse(s, StringUtils.capitalizeFirstPerWord(s))).zip(stop :+ "").map { case (p, s) => p + s }.mkString
        else
          StringUtils.capitalizeFirstPerWord(prenomNormal)

      def getPlace(code: String): Int = inseeCodePlaceMap.getOrElse(countryTranslation.getOrElse(code, code), 0)

      nomsSet.addAll(cleanSplitAndNormalize(p.nom))
      prenomsSet.addAll(cleanSplitAndNormalize(p.prenom))
      val (birthPlace, deathPlace) = (getPlace(p.birthCode), getPlace(p.deathCode))
      personsDataMap.put(id, PersonData(p.nom, prenomPretty, p.gender, p.birthDate, birthPlace, p.deathDate, deathPlace))

      Seq(birthPlace, deathPlace).flatMap(placeAbsolute).foreach { id =>
        placeOccurrences.update(id, placeOccurrences(id) + 1)
      }

      count += 1
    }

    logOK()

    namesAccent = null
    inseeCodePlaceMap = null
    countryTranslation = null

    logEllipse("Writing places index")

    write(placesIndex, idPlaceMap.keys.map(id => id -> (normalizeSentence(placeDisplay(placeAbsolute(id).map(idPlaceMap))), placeOccurrences(id))).toMap, placesIndexFile)

    logOK()

    placeOccurrences = null

    logEllipse("Attributing identifiers to names and surnames")

    var (nomsSorted, prenomsSorted) = (nomsSet.toSeq.sorted, prenomsSet.toSeq.sorted)

    logOK()

    nomsSet = null
    prenomsSet = null

    logEllipse("Building hashmap for names and surnames")

    var (nomsMap, prenomsMap) = (nomsSorted.zipWithIndex.toMap, prenomsSorted.zipWithIndex.toMap)

    logOK()

    logEllipse("Writing surnames index")

    write(genericNameIndex, nomsSorted.zipWithIndex.map(_.swap).toMap, surnamesIndexFile)

    logOK()

    logEllipse("Writing names index")

    write(genericNameIndex, prenomsSorted.zipWithIndex.map(_.swap).toMap, namesIndexFile)

    logOK()

    nomsSorted = null
    prenomsSorted = null

    logEllipse("Writing persons data")

    write(personsData, personsDataMap, personsDataFile)

    logOK()

    logEllipse("Building search data")

    val searchValues = personsDataMap.view.mapValues(p => PersonProcessed(cleanSplitAndNormalize(p.nom).map(nomsMap), cleanSplitAndNormalize(p.prenom).map(prenomsMap), p.gender, p.birthDate, placeAbsolute(p.birthPlaceId), p.deathDate, placeAbsolute(p.deathPlaceId))).toMap

    logOK()

    personsDataMap = null
    nomsMap = null
    prenomsMap = null
    idPlaceMap = null

    logEllipse("Writing search index (this will take some time)")

    write(searchIndex, searchValues, searchIndexFile)

    logOK()

    log("Successfully generated the database, exiting")

  }


  def dispose(): Unit = {
    personsDataFileIn.close()
    placesDataFileIn.close()
    surnamesIndexFileIn.close()
    namesIndexFileIn.close()
    searchIndexFileIn.close()
    placesIndexFileIn.close()
  }
}
