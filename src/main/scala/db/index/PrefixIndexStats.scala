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
    def queryNested(context: FileContextIn, childrenCount: Int, keyOpt: Option[Int], height: Int): Map[Int, Int] = {
      if(height > 0) {
        val pairs = for {
          i <- 0 until childrenCount
          localOffset = childrenCountHandler.Size + (keyHandler.Size + PointerSize) * i
          key = keyHandler.read(context, localOffset).toInt
          childPointer = context.readPointer(localOffset + keyHandler.Size)
          childContext = context.reindexAbsolute(childPointer)
          childBranchesCount = childrenCountHandler.read(childContext, 0).toInt
        } yield queryNested(childContext, childBranchesCount, Some(key), height - 1)
        pairs.fold(Map.empty)(_ ++ _)
      } else {
        val Some(key) = keyOpt // FIXME: poor design, should be ensured by types
        val childOffset = childrenCountHandler.Size + (keyHandler.Size + PointerSize) * childrenCount
        val count = context.readInt(childOffset)
        Map(key -> count)
      }
    }
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
        queryNested(context, childrenCount, None, nestingDepth(rest))
    }
  }

  def nestingDepth(q: Q): Int

  override private[db] final val child = null

  override private[db] def empty: Map[Int, Int] = Map.empty

}
