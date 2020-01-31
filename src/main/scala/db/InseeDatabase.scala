package db

import java.io.{File, RandomAccessFile}
import java.util.Date

import data.Person
import db.file.FileContext
import db.index.{ExactStringMachIndex, ExclusiveSubsetIndex}
import db.result.{DirectPersonResult, ReferenceResult}
import reader.InseePersonsReader

import scala.collection.Seq

class InseeDatabase(directory: String, readonly: Boolean = true) {

  private val root = new File(directory)
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

  type T = (Seq[Int], Seq[Int]) // TODO change this placeholder type

  private val personsData = new DirectPersonResult()

  // TODO places data

  private val genericNameIndex = new ExactStringMachIndex[String] {
    def getParameter(q: String): scala.collection.Seq[Byte] = q.getBytes
  }

  private val searchIndex = new ExclusiveSubsetIndex[T, ResultSet[Int]] {
    override def getParameter(q: T): Set[Int] = q._1.toSet
    override val child: DatabaseLevel[T, ResultSet[Int]] =
      new ExclusiveSubsetIndex[T, ResultSet[Int]] {
        override def getParameter(q: T): Set[Int] = q._2.toSet
        override val child: DatabaseLevel[T, ResultSet[Int]] = new ReferenceResult[T]()
        // TODO complete this
      }
  }

  private val placesIndex = ???


  /* Query methods */

  private def normalizeString(str: String): String = str.trim.toLowerCase

  private def nameToId(name: String): Option[Int] = genericNameIndex.queryFirst(namesIndexFile, normalizeString(name))

  private def surnameToId(surname: String): Option[Int] = genericNameIndex.queryFirst(surnamesIndexFile, normalizeString(surname))

  private def idToPerson(id: Int): Option[Person] = personsData.query(personsDataFile, id, 1, null).entries.headOption

  // TODO missing methods

  def queryPersons(limit: Int, offset: Int,
                   name: Option[String] = None,
                   surname: Option[String] = None,
                   placeId: Option[Int] = None,
                   birthAfter: Option[Date] = None, birthBefore: Option[Date] = None,
                   deathAfter: Option[Date] = None, deathBefore: Option[Date] = None): Either[Seq[Person], String] = {
    require(limit >= 0 && offset >= 0)

    val (namesOpt, surnamesOpt) = (name.map(cleanSplit), surname.map(cleanSplit))

    //val processed = ProcessedPerson()

    ???
  }

  def queryPlace(limit: Int, prefix: String): Seq[String] = { // TODO wrong return type

    ???
  }

  private def cleanSplit(str: String): IndexedSeq[String] = str.toLowerCase.trim.split("[^a-z]+").toVector.filter(_.nonEmpty)

  def generateDatabase(inseeFilename: String): Unit = {
    val iterator = InseePersonsReader.readCompiledFile(relativePath(inseeFilename))

    import scala.collection._

    val (nomsSet, prenomsSet) = (mutable.HashSet.empty[String], mutable.HashSet.empty[String])
    var count = 0
    iterator.foreach { p =>
      nomsSet.add(p.nom)
      prenomsSet.add(p.prenom)
      count += 1
    }


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
