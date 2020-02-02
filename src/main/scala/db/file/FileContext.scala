package db.file

import java.io.RandomAccessFile

import db.util.DatabaseUtils

import scala.collection._
import db.util.DatabaseUtils._

class FileContext(file: RandomAccessFile, val start: Long = 0) {

  def getOffset: Long = start

  private def seek(i: Long): Unit = {
    file.seek(start + i)
  }

  def readLong(i: Long): Long = {
    seek(i)
    file.readLong()
  }

  def readInt(i: Long): Int = {
    seek(i)
    file.readInt()
  }

  def readByte(i: Long): Int = {
    seek(i)
    file.readByte()
  }

  def readPointer(i: Long): Long = {
    seek(i)
    val high = file.readByte()
    val low = file.readInt()
    ((high.toLong & 0xff) << 32) | (low.toLong & 0xffffffff)
  }

  def readString(i: Long): (String, FileContext) = {
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



  def writeLong(i: Long, v: Long): Unit = {
    seek(i)
    file.writeLong(v)
  }

  def writeInt(i: Long, v: Int): Unit = {
    seek(i)
    file.writeInt(v)
  }

  def writeByte(i: Long, v: Int): Unit = {
    seek(i)
    file.writeByte(v)
  }

  def writePointer(i: Long, v: Long): Unit = {
    seek(i)
    file.writeByte((v >> 32).toInt)
    file.writeInt(v.toInt)
  }

  def writeString(i: Long, str: String): FileContext = {
    val bytes = str.getBytes
    (bytes :+ 0.toByte).zipWithIndex.foreach { case (b, i) =>
      writeByte(i, b)
    }
    reindex(bytes.size + 1)
  }

  def reindex(i: Long): FileContext = reindexAbsolute(start + i)

  def reindexAbsolute(i: Long): FileContext = new FileContext(file, i)


  def close(): Unit = {
    file.close()
  }

}
