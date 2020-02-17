package db.result

import java.util.{Calendar, Date}

import data.PersonData
import db.file.{FileContextIn, FileContextOut}
import db.{LevelResult, ResultSet}

import scala.collection.Map
import db.util.DatabaseUtils.{ByteSize, IntSize}

class DirectDateResult extends LevelResult[(Int, Int), PersonData, Option[Int]] {

  val BaseYear = 1850

  private val calendar = Calendar.getInstance()

  override def query(context: FileContextIn, offset: Int, limit: Int, parameters: (Int, Int)): Option[Int] = {
    val (dateType, id) = parameters
    val count = context.readInt(0)
    if(id < count && offset == 0 && limit > 0) {
      val base = IntSize + id * 2 * ByteSize
      val address = base + dateType * ByteSize
      Some(context.readByte(address) & 0xff)
    } else {
      empty
    }
  }

  override private[db] def empty: Option[Int] = None

  override def write(context: FileContextOut, data: Map[Int, PersonData]): Unit = {
    def dateToByte(date: Date): Int = {
      calendar.setTime(date)
      calendar.get(Calendar.YEAR) - BaseYear
    }

    val seq = data.keys.toSeq.sorted

    context.writeInt(seq.size)

    seq.foreach { id =>
      val p = data(id)
      context.writeByte(p.birthDate.map(dateToByte).getOrElse(0))
      context.writeByte(p.deathDate.map(dateToByte).getOrElse(0))
    }
  }
}
