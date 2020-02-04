package db.index

import db.LevelIndexParent
import db.file.FileContext
import db.util.DatabaseUtils.{PointerSize, binarySearch}

import scala.annotation.tailrec
import scala.collection.{Map, Seq}

abstract class AbstractFullTrieIndex[Q, P, R, T] extends LevelIndexParent[Q, P, R, T] with TrieBasedIndex {

  val ignoreRoot: Boolean = false

  def canonicalize(t: T): Seq[Int]

  def reorder(t: T): Seq[Seq[Int]]

  override def query(context: FileContext, offset: Int, limit: Int, parameters: Q): R = {
    val canonical = canonicalize(getQueryParameter(parameters))
    queryInternal(context, offset, limit, canonical, parameters)
  }

  @tailrec
  private def queryInternal(context: FileContext, offset: Int, limit: Int, values: Seq[Int], rest: Q): R = {
    val childrenCount = childrenCountHandler.read(context, 0).toInt
    values match {
      case head +: tail =>
        binarySearch(keyHandler.read(context, _).toInt, childrenCountHandler.Size, childrenCount, keyHandler.Size + PointerSize, head) match {
          case Some(resultIndex) => // Continue search, result set is decreasing
            val nextPointer = context.readPointer(resultIndex + keyHandler.Size)
            queryInternal(context.reindexAbsolute(nextPointer), offset, limit, tail, rest)
          case None => // Stop search, result set is empty
            empty
        }
      case Seq() => // End of search for this level, return current result set
        child.query(context.reindex(childrenCountHandler.Size + (keyHandler.Size + PointerSize) * childrenCount), offset, limit, rest)
    }
  }

  override def write(context: FileContext, data: Map[Int, P]): FileContext = {
    import scala.collection._

    class PrefixTrie(val children: mutable.HashMap[Int, PrefixTrie] = mutable.HashMap.empty, val values: mutable.Set[Int] = mutable.Set.empty, isRoot: Boolean = false) {
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
            if(!isRoot)
              values.add(value)
            child.insert(tail, value)
          case Seq() => if(!isRoot) values.add(value)
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

    val root = new PrefixTrie(isRoot = ignoreRoot)

    data.foreach { case (id, p) =>
      val set = getWriteParameter(p)
      reorder(set).foreach(variant => root.insert(variant, id))
    }

    def writeTrie(trie: PrefixTrie, context: FileContext): FileContext = {
      val sorted = trie.children.keys.toSeq.sorted
      val count = sorted.size
      childrenCountHandler.write(context, 0, count)
      val valuesMap = trie.values.map(v => v -> data(v)).toMap
      var start = child.write(context.reindex(childrenCountHandler.Size + (keyHandler.Size + PointerSize) * count), valuesMap)
      sorted.zipWithIndex.foreach { case (v, i) =>
        val child = trie.children(v)
        keyHandler.write(context, childrenCountHandler.Size + (keyHandler.Size + PointerSize) * i, v)
        context.writePointer(childrenCountHandler.Size + (keyHandler.Size + PointerSize) * i + keyHandler.Size, start.getOffset)
        start = writeTrie(child, start)
      }
      start
    }

    writeTrie(root, context)
  }
}
