package db.file

import java.io.RandomAccessFile

import scala.collection._

import db.util.DatabaseUtils._

class FileContext(file: RandomAccessFile, val start: Int = 0) {

  def getOffset: Int = start

  private def seek(i: Int): Unit = {
    file.seek(start + i)
  }

  def readInt(i: Int): Int = {
    seek(i)
    file.readInt()
  }

  def readByte(i: Int): Int = {
    seek(i)
    file.readByte()
  }

  def readString(i: Int): (String, FileContext) = {
    var chars: Vector[Byte] = Vector.empty
    var i = 0
    var b = readByte(i)
    while(b != 0) {
      chars :+= b.toByte
      i += ByteSize
      b = readByte(i)
    }
    (new String(chars.toArray), reindex(i + ByteSize))
  }

  def writeInt(i: Int, v: Int): Unit = {
    seek(i)
    file.writeInt(v)
  }

  def writeByte(i: Int, v: Int): Unit = {
    seek(i)
    file.writeByte(v)
  }

  def writeString(i: Int, str: String): FileContext = {
    val bytes = str.getBytes
    (bytes :+ 0).zipWithIndex.foreach { case (b, i) =>
      writeByte(i, b)
    }
    reindex(bytes.size + 1)
  }

  def reindex(i: Int): FileContext = reindexAbsolute(start + i)

  def reindexAbsolute(i: Int): FileContext = new FileContext(file, i)


}
