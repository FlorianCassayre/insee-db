package db.result

import db.{LevelResult, ResultSet}
import db.file.{FileContextIn, FileContextOut}
import db.util.DatabaseUtils.IntSize

import scala.collection.Map

abstract class LimitedReferenceResult[Q, P] extends LevelResult[Q, P, ResultSet[Int]] {

  val MaxResults: Int

  def ordering(id: Int, p: P): Int = id // Default implementation

  override private[db] def readResult(context: FileContextIn, offset: Int, limit: Int): ResultSet[Int] = {
    val resultSize = context.readInt(0)
    val resultPresent = Math.min(resultSize, MaxResults)
    val start = 1 + offset
    val end = Math.min(start + limit, 1 + resultPresent)
    val entries = (start until end).map(i => context.readInt(i * IntSize))

    ResultSet(entries, resultSize)
  }

  override private[db] def empty: ResultSet[Int] = ResultSet(Seq.empty, 0)

  override def write(context: FileContextOut, data: Map[Int, P]): Unit = {
    val sorted = data.keys.toSeq.sortBy(k => ordering(k, data(k)))
    val count = sorted.size
    val countPresent = Math.min(count, MaxResults)
    context.writeInt(countPresent)
    sorted.take(MaxResults).foreach(v => context.writeInt(v))
  }

}