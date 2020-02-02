package db.result

import db.file.FileContext
import db.{LevelResult, ResultSet}

import db.util.DatabaseUtils._

import collection._

abstract class DirectMappingResult[Q] extends LevelResult[Q, Q, ResultSet[Q]] {

  override private[db] def readResult(context: FileContext, offset: Int, limit: Int): ResultSet[Q] = {
    val count = context.readInt(0)
    val end = Math.min(offset + limit, count)
    val pointers = (offset until end).map(i => context.readPointer(IntSize + i * PointerSize))
    val seq = pointers.map(p => readResultEntry(context.reindexAbsolute(p)))

    ResultSet(seq, count)
  }

  def readResultEntry(context: FileContext): Q

  override private[db] def empty: ResultSet[Q] = ResultSet(Seq.empty, 0)

  override def write(context: FileContext, data: Map[Int, Q]): FileContext = {
    val seq = data.toIndexedSeq.sortBy(_._1)
    val count = seq.size
    context.writeInt(0, count)
    var start = context.reindex(IntSize + count * PointerSize)
    seq.zipWithIndex.foreach { case ((id, v), i) =>
      assert(id == i)
      context.writePointer(IntSize + i * PointerSize, start.getOffset)
      start = writeResultEntry(start, v)
    }
    start
  }

  def writeResultEntry(context: FileContext, entry: Q): FileContext

}
