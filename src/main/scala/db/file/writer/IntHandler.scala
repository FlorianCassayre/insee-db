package db.file.writer

import db.file.{FileContextIn, FileContextOut}
import db.util.DatabaseUtils.IntSize

class IntHandler extends DataHandler {
  override val Size: Int = IntSize
  override def write(context: FileContextOut, value: Long): Unit = context.writeInt(value.toInt)
  override def read(context: FileContextIn, offset: Long): Long = context.readInt(offset)
}
