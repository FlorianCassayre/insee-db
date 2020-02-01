package db.result

import data.PlaceData
import db.file.FileContext

import db.util.DatabaseUtils._

class DirectPlaceResult extends DirectMappingResult[PlaceData] {

  override def readResultEntry(context: FileContext): PlaceData = {
    def readIntOption(context: FileContext, offset: Int): Option[Int] = {
      val v = context.readInt(offset)
      if(v >= 0) Some(v) else None
    }
    val (name, next) = context.readString(0)
    val parent = readIntOption(next, 0)
    PlaceData(name, parent)
  }

  override def writeResultEntry(context: FileContext, entry: PlaceData): FileContext = {
    def writeIntOption(context: FileContext, offset: Int, option: Option[Int]): Unit = context.writeInt(offset, option.getOrElse(-1))
    val next = context.writeString(0, entry.name)
    writeIntOption(next, 0, entry.parent)
    next.reindex(IntSize)
  }

}
