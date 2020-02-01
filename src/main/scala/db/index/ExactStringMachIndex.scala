package db.index

import db.{LevelIndex, LevelIndexParent}
import db.file.FileContext
import db.util.DatabaseUtils._

import scala.annotation.tailrec
import scala.collection.{+:, Seq, mutable, Map}

abstract class ExactStringMachIndex[Q] extends LevelIndex[Q, Option[Int], Seq[Byte]] {

  private val MaskFlag = 0x80
  private val MaskCount = ~MaskFlag & 0xff

  override def query(context: FileContext, offset: Int, limit: Int, parameters: Q): Option[Int] =
    queryInternal(context, offset, limit, getParameter(parameters))

  @tailrec
  private def queryInternal(context: FileContext, offset: Int, limit: Int, parameter: Seq[Byte]): Option[Int] = {
    val header = context.readByte(0)
    val (flag, count) = (header & MaskFlag, header & MaskCount)
    val containsValue = flag != 0
    parameter match {
      case head +: tail =>
        val headerOffset = ByteSize + (if(containsValue) IntSize else 0)
        binarySearch(context.readByte, headerOffset, count, ByteSize + IntSize, head.toInt) match {
          case Some(value) =>
            val nextPointer = context.readInt(value + ByteSize)
            queryInternal(context.reindexAbsolute(nextPointer), offset, limit, tail)
          case None =>
            None
        }
      case Seq() => if(containsValue) Some(context.readInt(ByteSize)) else None
    }
  }

  override def write(context: FileContext, data: Map[Int, Q]): FileContext = {

    class Trie(val children: mutable.HashMap[Byte, Trie] = mutable.HashMap.empty, var value: Option[Int] = None) {
      def insert(key: Seq[Byte], v: Int): Unit = {
        key match {
          case head +: tail =>
            val child = children.get(head) match {
              case Some(child) => child
              case None =>
                val child = new Trie()
                children.put(head, child)
                child
            }
            child.insert(tail, v)
          case Seq() =>
            assert(value.isEmpty)
            value = Some(v)
        }
      }
      def query(key: Seq[Byte]): Option[Int] = {
        key match {
          case head +: tail =>
            children.get(head) match {
              case Some(child) => child.query(tail)
              case None => None
            }
          case Seq() => value
        }
      }
    }

    val root = new Trie()

    data.foreach { case (id, v) =>
      root.insert(getParameter(v), id)
    }

    def writeTrie(trie: Trie, context: FileContext): FileContext = {
      val count = trie.children.size
      val flag = trie.value.isDefined
      val header = (count & MaskCount) | (if(flag) MaskFlag else 0)
      context.writeByte(0, header)
      if(flag) {
        context.writeInt(ByteSize, trie.value.get)
      }
      val headerOffset = ByteSize + (if(flag) IntSize else 0)
      var start = context.reindex(headerOffset + (ByteSize + IntSize) * count)
      trie.children.keys.toSeq.sorted.zipWithIndex.foreach { case (k, i) =>
        val child = trie.children(k)
        val off = headerOffset + (ByteSize + IntSize) * i
        context.writeByte(off, k)
        context.writeInt(off + ByteSize, start.getOffset)
        start = writeTrie(child, start)
      }
      start
    }

    writeTrie(root, context)

  }

  override private[db] def empty: Option[Int] = None

}
