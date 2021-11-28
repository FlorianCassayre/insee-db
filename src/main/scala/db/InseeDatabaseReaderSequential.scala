package db

import data.{PersonData, PersonDisplay}
import db.util.DatabaseUtils._
import db.util.DateUtils

import java.io.{BufferedInputStream, DataInputStream, File, FileInputStream}
import java.time.LocalDate
import scala.collection.mutable

class InseeDatabaseReaderSequential(root: File) extends InseeDatabaseReader(root) {

  def personsDataIterator(): Iterable[PersonData] = {
    val reader = new DataInputStream(new BufferedInputStream(new FileInputStream(personsDataFile)))

    val n = reader.readInt() // Count
    def skipUntil(l: Long): Unit = {
      if(l > 0) {
        val actuallySkipped = reader.skip(l)
        val remaining = l - actuallySkipped
        assert(remaining >= 0)
        skipUntil(remaining)
      }
    }
    skipUntil(n * PointerSize) // Pointers

    // Begin data
    (0 until n).view.map { _ =>
      def readString(): String = {
        var chars = mutable.ArrayBuffer.empty[Byte]
        var i = 0
        var b = reader.readByte()
        while(b != 0) {
          chars :+= b
          i += ByteSize
          b = reader.readByte()
        }
        new String(chars.toArray)
      }
      def readDateOption(): Option[LocalDate] = {
        val v = reader.readLong()
        if(v != 0) Some(DateUtils.fromMillis(v)) else None
      }
      def readStringMapOption(): Option[Map[String, String]] = {
        val n = reader.readByte()
        if(n > 0) {
          val seq = (0 until n).foldLeft(Seq.empty[(String, String)])((acc, _) => (readString(), readString()) +: acc)
          Some(seq.toMap)
        } else {
          None
        }
      }

      val noms = readString()
      val prenoms = readString()
      val gender = reader.readByte() == 1
      val birthDate = readDateOption()
      val birthPlace = reader.readInt()
      val deathDate = readDateOption()
      val deathPlace = reader.readInt()
      val actCode = readString()
      val wikipedia = readStringMapOption()

      PersonData(noms, prenoms, gender, birthDate, birthPlace, deathDate, deathPlace, actCode, wikipedia)
    }
  }

  private val placeCache = mutable.Map.empty[Int, Option[String]] // Cache to accelerate lookups
  private def getPlace(id: Int): Option[String] = placeCache.getOrElseUpdate(id, idToPlaceDisplay(id))

  def personsDisplayIterator(): Iterable[PersonDisplay] = {
    personsDataIterator().map(p => PersonDisplay(p.nom, p.prenom, p.gender, p.birthDate, getPlace(p.birthPlaceId), p.deathDate, getPlace(p.deathPlaceId), p.actCode, p.wikipedia))
  }

}
