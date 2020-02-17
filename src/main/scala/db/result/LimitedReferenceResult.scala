package db.result

import db.{LevelResult, ResultSet}
import db.file.{FileContextIn, FileContextOut}
import db.util.DatabaseUtils
import db.util.DatabaseUtils.IntSize

import scala.collection.Map
import scala.collection.immutable.NumericRange

abstract class LimitedReferenceResult[Q, P] extends LevelResult[Q, P, ResultSet[Int]] {

  val MaxResults: Int

  // Default implementations

  val OrdersCount: Int = 1

  def ordering(i: Int)(id: Int, value: P): Long = id

  def getOrder(q: Q): Int = 0

  def orderTransformer(i: Int)(id: Int): Int = id

  def lowerBound(i: Int)(value: Q): Option[Int] = None

  def upperBound(i: Int)(value: Q): Option[Int] = None

  def isAscending(i: Int)(value: Q): Boolean = true

  override def query(context: FileContextIn, offset: Int, limit: Int, parameters: Q): ResultSet[Int] = {
    val order = getOrder(parameters)
    val completeResultSize = context.readInt(0)
    val resultPresent = Math.min(completeResultSize, MaxResults)
    val orderingOffset = order * resultPresent * IntSize
    val start = IntSize.toLong + orderingOffset

    val lower: Option[Long] = lowerBound(order)(parameters) match {
      case Some(needle) => DatabaseUtils.binarySearchLowerBound(context.readInt, orderTransformer(order), start, resultPresent, IntSize, needle).map(start + _ * IntSize)
      case None => Some(start)
    }
    val upper: Option[Long] = upperBound(order)(parameters) match {
      case Some(needle) => DatabaseUtils.binarySearchUpperBound(context.readInt, orderTransformer(order), start, resultPresent, IntSize, needle).map(_ + 1).map(start + _ * IntSize)
      case None => Some(start + resultPresent * IntSize)
    }

    (lower, upper) match {
      case (Some(low), Some(high)) => // Range is defined
        val range = NumericRange(low, high, IntSize) // (syntax for ranges using Longs)
        val allEntriesSorted = if(isAscending(order)(parameters)) range else range.reverse
        val entries = allEntriesSorted.slice(offset, offset + limit).map(context.readInt)
        ResultSet(entries, range.size)
      case _ =>
        empty // One of the bound cuts off all results
    }
  }

  override private[db] def empty: ResultSet[Int] = ResultSet(Seq.empty, 0)

  override def write(context: FileContextOut, data: Map[Int, P]): Unit = {
    val seq = data.keys.toSeq
    val count = seq.size
    val countPresent = Math.min(count, MaxResults)
    context.writeInt(countPresent)
    for(i <- 0 until OrdersCount) {
      val by: (Int, P) => Long = ordering(i)
      val sorted = seq.sortBy(k => by(k, data(k)))
      sorted.take(MaxResults).foreach(v => context.writeInt(v))
    }
  }

}