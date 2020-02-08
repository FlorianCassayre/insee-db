package db.result

import db.file.{FileContextIn, FileContextOut}
import db.{LevelResult, ResultSet}
import db.util.DatabaseUtils._

import collection._

abstract class DirectMappingResult[Q] extends LevelResult[Q, Q, ResultSet[Q]] {

  override private[db] def readResult(context: FileContextIn, offset: Int, limit: Int): ResultSet[Q] = {
    val count = context.readInt(0)
    val end = Math.min(offset + limit, count)
    val pointers = (offset until end).map(i => context.readPointer(IntSize + i * PointerSize))
    val seq = pointers.map(p => readResultEntry(context.reindexAbsolute(p)))

    ResultSet(seq, count)
  }

  def readResultEntry(context: FileContextIn): Q

  override private[db] def empty: ResultSet[Q] = ResultSet(Seq.empty, 0)

  override def write(context: FileContextOut, data: Map[Int, Q]): Unit = {
    val seq = data.toIndexedSeq.sortBy(_._1)
    val count = seq.size
    context.writeInt(count)
    val sortedWithAddress = seq.zipWithIndex.map { case ((id, v), i) =>
      assert(id == i)
      val address = context.getOffset
      context.writeEmptyPointer()
      (v, address)
    }
    sortedWithAddress.foreach { case (v, address) =>
      val valueAddress = context.getOffset
      context.bufferAppendPointer(address, valueAddress)
      writeResultEntry(context, v)
    }
  }

  def writeResultEntry(context: FileContextOut, entry: Q): Unit

}
