package db.file

import java.io.RandomAccessFile

import db.util.DatabaseUtils._

class FileContextIn(file: RandomAccessFile, start: Long = 0) {

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
    ((high & 0xffL) << 32) | (low & 0xffffffffL)
  }

  def readString(i: Long): (String, FileContextIn) = {
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


  def reindex(i: Long): FileContextIn = reindexAbsolute(start + i)

  def reindexAbsolute(i: Long): FileContextIn = new FileContextIn(file, i)


  def close(): Unit = {
    file.close()
  }

}
