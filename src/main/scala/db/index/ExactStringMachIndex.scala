package db.index

import db.LevelIndex
import db.file.{FileContextIn, FileContextOut}
import db.util.DatabaseUtils._

import scala.annotation.tailrec
import scala.collection.{+:, Map, Seq, mutable}

// TODO duplicated code
abstract class ExactStringMachIndex[Q, P] extends LevelIndex[Q, P, Option[Int], Seq[Byte]] {

  private val MaskFlag = 0x80
  private val MaskCount = ~MaskFlag & 0xff

  override def query(context: FileContextIn, offset: Int, limit: Int, parameters: Q): Option[Int] =
    queryInternal(context, offset, limit, getQueryParameter(parameters))

  @tailrec
  private def queryInternal(context: FileContextIn, offset: Int, limit: Int, parameter: Seq[Byte]): Option[Int] = {
    val header = context.readByte(0)
    val (flag, count) = (header & MaskFlag, header & MaskCount)
    val containsValue = flag != 0
    parameter match {
      case head +: tail =>
        val headerOffset = ByteSize + (if(containsValue) IntSize else 0)
        binarySearch(context.readByte, headerOffset, count, ByteSize + PointerSize, head.toInt) match {
          case Some(value) =>
            val nextPointer = context.readPointer(value + ByteSize)
            queryInternal(context.reindexAbsolute(nextPointer), offset, limit, tail)
          case None =>
            None
        }
      case Seq() => if(containsValue) Some(context.readInt(ByteSize)) else None
    }
  }

  override def write(context: FileContextOut, data: Map[Int, P]): Unit = {

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
      root.insert(getWriteParameter(v), id)
    }

    def writeTrie(trie: Trie): Unit = {
      val count = trie.children.size
      val flag = trie.value.isDefined
      val header = (count & MaskCount) | (if(flag) MaskFlag else 0)
      context.writeByte(header)
      if(flag) {
        context.writeInt(trie.value.get)
      }
      val sorted = trie.children.keys.toSeq.sorted
      val sortedWithAddress = sorted.map { k =>
        context.writeByte(k)
        val address = context.getOffset
        context.writeEmptyPointer()
        (k, address)
      }

      sortedWithAddress.foreach { case (k, address) =>
        val child = trie.children(k)
        val childAddress = context.getOffset
        context.bufferAppendPointer(address, childAddress)
        writeTrie(child)
      }
    }

    writeTrie(root)
  }

  override private[db] def empty: Option[Int] = None

}
