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

    // Group by -> count
    // Data is sorted and dense
    // Divide and conquer algorithm

    val transformer = orderTransformer(order)(_)
    def attributeAt(i: Long): Int = transformer(context.readInt(i))

    val (lowerMap, upperMap) = (mutable.Map.empty[Int, Long], mutable.Map.empty[Int, Long])
    def addLower(k: Int, v: Long): Unit = lowerMap += k -> lowerMap.get(k).map(Math.min(_, v)).getOrElse(v)
    def addUpper(k: Int, v: Long): Unit = upperMap += k -> upperMap.get(k).map(Math.max(_, v)).getOrElse(v)

    val two = 2 * IntSize

    def aggregate(start: Long, end: Long, startValue: Option[Int] = None, endValue: Option[Int] = None): Unit = {
      val diff = end - start
      if(diff > 0) {
        val first = startValue.getOrElse(attributeAt(start))
        if(diff == IntSize) {
          addLower(first, start)
          addUpper(first, start)
        } else {
         val last = endValue.getOrElse(attributeAt(end - IntSize))
          if(first == last) {
            addLower(first, start)
            addUpper(first, start)
          } else if(diff == two) { // Prevent infinite loop
            addLower(first, start)
            addUpper(first, start)
            val second = start + IntSize
            addLower(last, second)
            addUpper(last, second)
          } else { // Divide
            val mid = ((start + end) >> (1 + 2)) << 2
            val midValue = attributeAt(mid)
            aggregate(start, mid + IntSize, endValue = Some(midValue))
            aggregate(mid, end, startValue = Some(midValue))
          }
        }
      } // Otherwise nothing
    }

    aggregate(start, end)

    lowerMap.map { case (k, v) => k -> (((upperMap(k) - v) / IntSize).toInt + 1) }
  }

  override private[db] def empty: Map[Int, Int] = Map.empty

  override def write(context: FileContextOut, data: collection.Map[Int, P]): Unit = throw new UnsupportedOperationException

}
