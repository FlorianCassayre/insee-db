package db

import java.io.File

import data.{PersonData, PersonProcessed, PersonRaw, PlaceData, PlaceTree}
import db.file.FileContextOut
import db.util.StringUtils
import db.util.StringUtils.{cleanSplitAndNormalize, normalizeSentence, normalizeString}
import reader.{InseeNamesReader, InseePersonsReader, InseePlacesReader}

import scala.annotation.tailrec
import scala.collection.{Map, Seq}

class InseeDatabaseWriter(root: File) extends AbstractInseeDatabase(root) {

  private def write[P](level: DatabaseLevel[_, P, _], data: Map[Int, P], file: File): Unit = {
    val bufferSizeBytes = 1_000_000_000 // TODO
    val context = new FileContextOut(file, bufferSizeBytes)
    level.write(context, data)
    print("Flushing... ")
    context.close()
  }

  def generateDatabase(inseeSourceFilesDirectory: File): Unit = {
    require(inseeSourceFilesDirectory.isDirectory)
    def file(subpath: String): File = new File(inseeSourceFilesDirectory.getAbsolutePath + File.separator + subpath)
    generateDatabase(
      file("deaths"),
      file("places"),
      file("prenoms.csv")
    )
  }

  def generateDatabase(inseeOfficialFilesDirectory: File, inseePlaceDirectory: File, inseeNamesFiles: File): Unit = {
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

    def getIterator(): Iterable[PersonRaw] =
      inseeOfficialFilesDirectory.listFiles().sortBy(_.getName).view
        .flatMap(InseePersonsReader.readOfficialYearlyFile)
        .filter(InseePersonsReader.isReasonable)
    //.take(1_000)

    def getPlace(code: String): Int = inseeCodePlaceMap.getOrElse(countryTranslation.getOrElse(code, code), 0)

    val stopRegex = "[^a-z]+".r
    var (nomsSet, prenomsSet) = (mutable.HashSet.empty[String], mutable.HashSet.empty[String])
    var personsDataMap = mutable.Map.empty[Int, PersonData]
    var personsSet = mutable.Set.empty[PersonData]
    var placeOccurrences = mutable.Seq.fill(idPlaceMap.size)(0)
    var count = 0
    var duplicatesRemoved = 0
    getIterator().foreach { p =>
      val id = count

      if(id % 400000 == 0) {
        println(id)
        System.gc()
      }

      val prenomNormal = normalizeString(p.prenom)
      val stop = stopRegex.findAllIn(prenomNormal).toSeq
      val split = stopRegex.split(prenomNormal)
      val prenomPretty =
        if(stop.size == split.length - 1)
          split.map(s => namesAccent.getOrElse(s, StringUtils.capitalizeFirstPerWord(s))).zip(stop :+ "").map { case (p, s) => p + s }.mkString
        else
          StringUtils.capitalizeFirstPerWord(prenomNormal)


      nomsSet.addAll(cleanSplitAndNormalize(p.nom))
      prenomsSet.addAll(cleanSplitAndNormalize(p.prenom))
      val (birthPlace, deathPlace) = (getPlace(p.birthCode), getPlace(p.deathCode))
      val personData = PersonData(p.nom, prenomPretty, p.gender, p.birthDate, birthPlace, p.deathDate, deathPlace)
      if(!personsSet.contains(personData)) {
        personsDataMap.put(id, personData)
        personsSet.add(personData)

        Seq(birthPlace, deathPlace).flatMap(placeAbsolute).foreach { id =>
          placeOccurrences.update(id, placeOccurrences(id) + 1)
        }

        count += 1
      } else {
        duplicatesRemoved += 1
      }
    }

    personsSet = null
    System.gc()

    logOK()
    println(s"Loaded $count individuals, removed $duplicatesRemoved duplicates")

    namesAccent = null

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

    logEllipse("Writing dates data")

    write(datesData, personsDataMap, datesDataFile)

    logOK()

    logEllipse("Writing persons data")

    write(personsData, personsDataMap, personsDataFile)

    logOK()

    personsDataMap = null

    System.gc() // TODO

    val total = count

    logEllipse("Building search data")

    var searchValues: mutable.Map[Int, PersonProcessed] = mutable.Map.empty

    count = 0
    getIterator().foreach { p =>
      val id = count

      if(id % 400000 == 0) {
        println(id)
        System.gc()
      }

      val (birthPlace, deathPlace) = (getPlace(p.birthCode), getPlace(p.deathCode))
      val processed = PersonProcessed(cleanSplitAndNormalize(p.nom).map(nomsMap).toArray, cleanSplitAndNormalize(p.prenom).map(prenomsMap).toArray, p.gender, p.birthDate, placeAbsolute(birthPlace), p.deathDate, placeAbsolute(deathPlace))
      searchValues.put(id, processed)
      count += 1
    }

    logOK()

    countryTranslation = null
    inseeCodePlaceMap = null
    nomsMap = null
    prenomsMap = null
    idPlaceMap = null

    System.gc()

    logEllipse("Writing search index (this will take some time)")

    write(searchIndex, searchValues, searchIndexFile)

    logOK()

    log("Successfully generated the database, exiting")

  }

  override protected def idToDate(id: Int, kind: Int): Option[Int] = throw new UnsupportedOperationException


}
