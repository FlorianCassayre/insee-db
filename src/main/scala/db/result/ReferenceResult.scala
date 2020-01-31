package db.result

import db.{LevelResult, ResultSet}
import db.file.FileContext
import db.util.DatabaseUtils._

class ReferenceResult[Q] extends LevelResult[Q, ResultSet[Int]] {

  override private[db] def readResult(context: FileContext, offset: Int, limit: Int): ResultSet[Int] = {
    val resultSize = context.readInt(0)
    val start = 1 + offset
    val end = Math.min(start + limit, 1 + resultSize)
    val entries = (start until end).map(i => context.readInt(i * IntSize))

    ResultSet(entries, resultSize)
  }

  override private[db] def empty: ResultSet[Int] = ResultSet(Seq.empty, 0)

  override def write(context: FileContext, data: Map[Int, Q]): FileContext = {
    val sorted = data.keys.toSeq.sorted // TODO not necessary to sort here but could be used for something else
    val count = sorted.size
    context.writeInt(0, count)
    sorted.zipWithIndex.foreach { case (v, i) =>
      context.writeInt((1 + i) * IntSize, v)
    }
    context.reindex((1 + count) * IntSize)
  }

}
