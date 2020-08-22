package db.result

import db.file.{FileContextIn, FileContextOut}
import db.util.DatabaseUtils
import db.util.DatabaseUtils.IntSize

import scala.collection.immutable.{NumericRange, TreeMap}
import scala.collection.{Map, mutable}

abstract class ReferenceResultStats[Q, P] extends OrderedReferenceResult[Q, P, Map[Int, Int]] {

  override def query(context: FileContextIn, offset: Int, limit: Int, parameters: Q): Map[Int, Int] = {
    val order = getOrder(parameters)
    val completeResultSize = context.readInt(0)
    val resultPresent = Math.min(completeResultSize, MaxResults)
    val offsetStep = resultPresent * IntSize
    val orderingOffset = order * offsetStep
    val start = IntSize.toLong + orderingOffset
    val end = start + offsetStep

    // Mutable cache: aggKey -> address
    val queue: mutable.TreeMap[Long, Int] = mutable.TreeMap.empty

    // Group by -> count
    // Data is sorted and dense, expected cost: O(k*log(n))
    // Reduced IO with memory caching
    @scala.annotation.tailrec
    def aggregate(betterStart: Long, aggregated: Map[Int, Int]): Map[Int, Int] = {
      val resultsLeft = NumericRange(betterStart, end, IntSize).size
      if(betterStart < end) {
        val transformer = orderTransformer(order)(_)
        val aggKey = transformer(context.readInt(betterStart))

        def updateCache(address: Long, key: Int): Unit = {
          if(key > aggKey) {
            queue.put(address, key)
          }
        }

        var betterSize = resultsLeft.toInt
        while(queue.nonEmpty && queue.head._2 <= aggKey) {
          queue.remove(queue.head._1)
        }
        if(queue.nonEmpty) {
          betterSize = NumericRange(betterStart, queue.head._1, IntSize).size
        }

        val Some(last) = DatabaseUtils.binarySearchUpperBound(context.readInt, transformer, betterStart, betterSize, IntSize, aggKey, Some(updateCache)).map(_ + 1).map(betterStart + _ * IntSize)
        val count = NumericRange(betterStart, last, IntSize).size
        aggregate(last, aggregated + (aggKey -> count))
      } else {
        aggregated
      }
    }
    aggregate(start, Map.empty)
  }

  override private[db] def empty: Map[Int, Int] = Map.empty

  override def write(context: FileContextOut, data: collection.Map[Int, P]): Unit = throw new UnsupportedOperationException

}
