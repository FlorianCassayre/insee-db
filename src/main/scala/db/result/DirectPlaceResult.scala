package db.result

import data.PlaceData
import db.file.{FileContextIn, FileContextOut}
import db.util.DatabaseUtils._

class DirectPlaceResult extends DirectMappingResult[PlaceData] {

  override def readResultEntry(context: FileContextIn): PlaceData = {
    def readIntOption(context: FileContextIn, offset: Int): Option[Int] = {
      val v = context.readInt(offset)
      if(v >= 0) Some(v) else None
    }
    val (name, next) = context.readString(0)
    val parent = readIntOption(next, 0)
    PlaceData(name, parent)
  }

  override def writeResultEntry(context: FileContextOut, entry: PlaceData): Unit = {
    def writeIntOption(option: Option[Int]): Unit = context.writeInt(option.getOrElse(-1))
    context.writeString(entry.name)
    writeIntOption(entry.parent)
  }

}
