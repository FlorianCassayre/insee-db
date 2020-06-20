package db.file

import java.io.{BufferedInputStream, BufferedOutputStream, DataInputStream, DataOutput, DataOutputStream, File, FileInputStream, FileOutputStream, RandomAccessFile}
import java.util
import java.util.{Collections, Comparator}

import db.util.DatabaseUtils._

class FileContextOut(file: File, maxBufferSize: Long) {

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

  def writeShort(s: Int): Unit = {
    outMain.writeShort(s)
    positionMain += ShortSize
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

    val bufferRead = new DataInputStream(new BufferedInputStream(new FileInputStream(bufferFile)))
    def ceilDiv(num: Long, divisor: Long) = (num + divisor - 1) / divisor

    val maxEntriesPerFragment = ceilDiv(maxBufferSize, 2 * LongSize)
    val fragments = ceilDiv(bufferLength, maxEntriesPerFragment).toInt
    assert(fragments < 100) // Safeguard

    def readBufferEntry(stream: DataInputStream): (Long, Long) = {
      def readPointer(): Long = {
        val high = stream.readByte()
        val low = stream.readInt()
        ((high & 0xffL) << 32) | (low & 0xffffffffL)
      }
      (readPointer(), readPointer())
    }

    def getFragmentFile(id: Int): File = new File(file.getAbsoluteFile + ".tmp" + id)

    val fragmentsLength = Array.ofDim[Int](fragments)
    var remaining = bufferLength.toLong
    for(fragment <- 0 until fragments) {
      val fragmentFile = getFragmentFile(fragment)
      val fragmentOutput = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(fragmentFile)))

      val newRemaining = Math.max(remaining - maxEntriesPerFragment, 0)
      val entriesInFragment = (remaining - newRemaining).toInt
      remaining = newRemaining
      fragmentsLength(fragment) = entriesInFragment

      // Populate array
      val array = new PointerArrayList(Array.ofDim[Long](2 * entriesInFragment.toInt))
      for(i <- 0 until entriesInFragment) {
        val entry = readBufferEntry(bufferRead)
        array.set(i, entry)
      }

      // Sort array
      Collections.sort(array, new PointerTupleComparator)

      // Write sorted array back to disk
      for(i <- 0 until entriesInFragment) {
        val (location, value) = array.get(i)
        writePointer(fragmentOutput, location)
        writePointer(fragmentOutput, value)
      }

      fragmentOutput.close()
    }

    bufferRead.close() // Close reader
    bufferFile.delete() // Delete temporary buffer

    val fragmentFiles = (0 until fragments).map(getFragmentFile)
    val fragmentInputs = fragmentFiles.map(f => new DataInputStream(new BufferedInputStream(new FileInputStream(f))))
    val fragmentIndex = Array.ofDim[Int](fragments)
    fragmentsLength.copyToArray(fragmentIndex)
    def readNext(id: Int): Option[(Long, Long)] = {
      if(fragmentIndex(id) > 0) {
        fragmentIndex(id) -= 1
        val fragmentInput = fragmentInputs(id)
        val entry = readBufferEntry(fragmentInput)
        Some(entry)
      } else {
        None
      }
    }

    // Initialize priority queue
    val queue = new util.TreeMap[(Long, Long), Int](new PointerTupleComparator)
    for {
      i <- 0 until fragments
      entry <- readNext(i)
    } {
      queue.put(entry, i)
    }

    val reader = new DataInputStream(new BufferedInputStream(new FileInputStream(file)))
    val copyFile = new File(file.getAbsolutePath + ".cpy")
    val copy = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(copyFile)))
    var currentAddress = 0L
    for(i <- 0 until bufferLength) {
      val entry = queue.pollFirstEntry()
      val (fileAddress, pointerValue) = entry.getKey
      val index = entry.getValue

      val len = (fileAddress - currentAddress + PointerSize).toInt
      val bytes = Array.ofDim[Byte](len)
      var r = 0
      while(r < len) {
        r += reader.read(bytes, r, len - r)
      }
      copy.write(bytes, 0, len - PointerSize)
      writePointer(copy, pointerValue)

      currentAddress += len

      // Refill the queue
      readNext(index).foreach { next =>
        queue.put(next, index)
      }
    }

    val len = (file.length() - currentAddress).toInt
    val bytes = Array.ofDim[Byte](len)
    var r = 0
    while(r < len) {
      r += reader.read(bytes, r, len - r)
    }
    copy.write(bytes, 0, len)
    currentAddress += len

    // Close and delete
    fragmentInputs.foreach(_.close())
    assert(fragmentFiles.forall(_.delete()))

    // Close
    reader.close()
    copy.close()

    assert(file.delete())
    assert(copyFile.renameTo(file))
  }

  private class PointerArrayList(array: Array[Long]) extends util.AbstractList[(Long, Long)] {
    assert(array.length % 2 == 0)
    private val realSize = array.length / 2

    override def get(index: Int): (Long, Long) = {
      val j = 2 * index
      (array(j), array(j + 1))
    }
    override def set(index: Int, e: (Long, Long)): (Long, Long) = {
      val j = 2 * index
      val previous = (array(j), array(j + 1))
      array(j) = e._1
      array(j + 1) = e._2
      previous
    }
    override def size(): Int = realSize
  }

  private class PointerTupleComparator extends Comparator[(Long, Long)] {
    override def compare(o1: (Long, Long), o2: (Long, Long)): Int = java.lang.Long.compare(o1._1, o2._1)
  }
}
