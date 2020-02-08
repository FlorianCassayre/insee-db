package db.file

import java.io.{BufferedInputStream, BufferedOutputStream, DataInputStream, DataOutput, DataOutputStream, File, FileInputStream, FileOutputStream, RandomAccessFile}

import db.util.DatabaseUtils._

class FileContextOut(file: File) {

  private val bufferFile: File = new File(file.getAbsoluteFile + ".tmp")

  private def openOut(file: File): DataOutputStream = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(file)))

  private val outMain: DataOutputStream = openOut(file)
  private val outPointerBuffer: DataOutputStream = openOut(bufferFile)

  private var positionMain: Long = 0

  private var bufferLength: Int = 0

  def writeByte(b: Int): Unit = {
    outMain.writeByte(b)
    positionMain += ByteSize
  }

  def writeInt(i: Int): Unit = {
    outMain.writeInt(i)
    positionMain += IntSize
  }

  def writeLong(l: Long): Unit = {
    outMain.writeLong(l)
    positionMain += LongSize
  }

  private def writePointer(out: DataOutput, ptr: Long): Unit = {
    out.writeByte((ptr >> 32).toInt)
    out.writeInt(ptr.toInt)
  }

  def writePointer(ptr: Long): Unit = {
    writePointer(outMain, ptr)
    positionMain += PointerSize
  }

  def writeEmptyPointer(): Unit = {
    writePointer(0)
  }

  def writeString(str: String): Unit = {
    val bytes = str.getBytes
    (bytes :+ 0.toByte).foreach(b => outMain.writeByte(b))
    positionMain += bytes.size + ByteSize
  }

  def bufferAppendPointer(fileOffset: Long, ptr: Long): Unit = {
    writePointer(outPointerBuffer, fileOffset)
    writePointer(outPointerBuffer, ptr)
    bufferLength += 1
  }

  def getOffset: Long = positionMain

  def close(): Unit = {
    // Flush and close
    outMain.close()
    outPointerBuffer.close()

    val randomAccess = new RandomAccessFile(file, "rw")
    val bufferRead = new DataInputStream(new BufferedInputStream(new FileInputStream(bufferFile)))

    def readBufferEntry(): (Long, Long) = {
      def readPointer(): Long = {
        val high = bufferRead.readByte()
        val low = bufferRead.readInt()
        ((high & 0xffL) << 32) | (low & 0xffffffffL)
      }
      (readPointer(), readPointer())
    }

    for(i <- 0 until bufferLength) {
      val (fileAddress, pointerValue) = readBufferEntry()
      randomAccess.seek(fileAddress)
      writePointer(randomAccess, pointerValue)
    }

    // Close
    randomAccess.close()
    bufferRead.close()

    bufferFile.delete() // Delete temporary buffer
  }

}
