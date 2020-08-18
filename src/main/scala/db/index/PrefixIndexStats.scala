package db.index

import db.file.FileContextIn
import db.util.DatabaseUtils.{PointerSize, binarySearch}

import scala.annotation.tailrec
import scala.collection.{Map, Seq, Set}

trait PrefixIndexStats[Q, P, T] extends AbstractFullTrieIndex[Q, P, Map[Int, Int], T] {

  override def query(context: FileContextIn, offset: Int, limit: Int, parameters: Q): Map[Int, Int] = {
    val canonical = canonicalize(getQueryParameter(parameters))
    queryInternal(context, 0, offset, limit, canonical, parameters)
  }

  @tailrec
  private def queryInternal(context: FileContextIn, depth: Int, offset: Int, limit: Int, values: Seq[Int], rest: Q): Map[Int, Int] = {
    val childrenCount = childrenCountHandler.read(context, 0).toInt
    values match {
      case head +: tail =>
        binarySearch(keyHandler.read(context, _).toInt, childrenCountHandler.Size, childrenCount, keyHandler.Size + PointerSize, head) match {
          case Some(resultIndex) => // Continue search, result set is decreasing
            val nextPointer = context.readPointer(resultIndex + keyHandler.Size)
            queryInternal(context.reindexAbsolute(nextPointer), depth + 1, offset, limit, tail, rest)
          case None => // Stop search, result set is empty
            empty
        }
      case Seq() => // End of search for this level, return current result set
        val pairs = for {
          i <- 0 until childrenCount
          localOffset = childrenCountHandler.Size + (keyHandler.Size + PointerSize) * i
          key = keyHandler.read(context, localOffset).toInt
          childPointer = context.readPointer(localOffset + keyHandler.Size)
          childContext = context.reindexAbsolute(childPointer)
          childBranchesCount = childrenCountHandler.read(childContext, 0).toInt
          childOffset = childrenCountHandler.Size + (keyHandler.Size + PointerSize) * childBranchesCount
          count = childContext.readInt(childOffset)
        } yield key -> count
        pairs.toMap
    }
  }

  override private[db] final val child = null

  override private[db] def empty: Map[Int, Int] = Map.empty

}
