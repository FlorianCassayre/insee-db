package db.result

import db.{LevelResult, ResultSet}
import db.file.FileContext

import db.util.DatabaseUtils.IntSize

import scala.collection.Map

abstract class LimitedReferenceResult[Q, P] extends LevelResult[Q, P, ResultSet[Int]] {

  val MaxResults: Int

  def ordering(id: Int, p: P): Int = id // Default implementation

  override private[db] def readResult(context: FileContext, offset: Int, limit: Int): ResultSet[Int] = {
    val resultSize = context.readInt(0)
    val resultPresent = Math.min(resultSize, MaxResults)
    val start = 1 + offset
    val end = Math.min(start + limit, 1 + resultPresent)
    val entries = (start until end).map(i => context.readInt(i * IntSize))

    ResultSet(entries, resultSize)
  }

  override private[db] def empty: ResultSet[Int] = ResultSet(Seq.empty, 0)

  override def write(context: FileContext, data: Map[Int, P]): FileContext = {
    val sorted = data.keys.toSeq.sortBy(k => ordering(k, data(k)))
    val count = sorted.size
    val countPresent = Math.min(count, MaxResults)
    context.writeInt(0, countPresent)
    sorted.take(MaxResults).zipWithIndex.foreach { case (v, i) =>
      context.writeInt((1 + i) * IntSize, v)
    }
    context.reindex((1 + countPresent) * IntSize)
  }

}