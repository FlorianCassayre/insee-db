package db.index

import db.LevelIndexParent
import db.file.FileContext
import db.util.DatabaseUtils.{IntSize, binarySearch}

import scala.annotation.tailrec

import collection.{Seq, Map}

abstract class AbstractFullTrieIndex[Q, P, R, T] extends LevelIndexParent[Q, P, R, T] {

  def canonicalize(t: T): Seq[Int]

  def reorder(t: T): Seq[Seq[Int]]

  override def query(context: FileContext, offset: Int, limit: Int, parameters: Q): R = {
    val canonical = canonicalize(getQueryParameter(parameters))
    queryInternal(context, offset, limit, canonical, parameters)
  }

  @tailrec
  private def queryInternal(context: FileContext, offset: Int, limit: Int, values: Seq[Int], rest: Q): R = {
    val childrenCount = context.readInt(0)
    values match {
      case head +: tail =>
        binarySearch(context.readInt, IntSize, childrenCount, 2 * IntSize, head) match {
          case Some(resultIndex) => // Continue search, result set is decreasing
            val nextPointer = context.readInt(resultIndex + IntSize)
            queryInternal(context.reindexAbsolute(nextPointer), offset, limit, tail, rest)
          case None => // Stop search, result set is empty
            empty
        }
      case Seq() => // End of search for this level, return current result set
        child.query(context.reindex(IntSize + 2 * IntSize * childrenCount), offset, limit, rest)
    }
  }

  override def write(context: FileContext, data: Map[Int, P]): FileContext = {
    val valuesMap = data.view.mapValues(getWriteParameter).toIndexedSeq

    import scala.collection._

    class PrefixTrie(val children: mutable.HashMap[Int, PrefixTrie] = mutable.HashMap.empty, val values: mutable.Set[Int] = mutable.Set.empty) {
      def insert(key: Seq[Int], value: Int): Unit = {
        key match {
          case head +: tail =>
            val child = children.get(head) match {
              case Some(child) => child
              case None =>
                val child = new PrefixTrie()
                children.put(head, child)
                child
            }
            values.add(value)
            child.insert(tail, value)
          case Seq() => values.add(value)
        }
      }
      def query(prefix: Seq[Int]): Set[Int] = {
        prefix match {
          case head +: tail =>
            children.get(head) match {
              case Some(child) => child.query(tail)
              case None => Set.empty
            }
          case Seq() => values
        }
      }
    }

    val root = new PrefixTrie()

    valuesMap.foreach { case (id, set) =>
      reorder(set).foreach(variant => root.insert(variant, id))
    }

    def writeTrie(trie: PrefixTrie, context: FileContext): FileContext = {
      val sorted = trie.children.keys.toSeq.sorted
      val count = sorted.size
      context.writeInt(0, count)
      val valuesMap = trie.values.map(v => v -> data(v)).toMap
      var start = child.write(context.reindex((1 + 2 * count) * IntSize), valuesMap)
      sorted.zipWithIndex.foreach { case (v, i) =>
        val child = trie.children(v)
        context.writeInt((1 + 2 * i) * IntSize, v)
        context.writeInt((1 + 2 * i + 1) * IntSize, start.getOffset)
        start = writeTrie(child, start)
      }
      start
    }

    writeTrie(root, context)
  }
}
