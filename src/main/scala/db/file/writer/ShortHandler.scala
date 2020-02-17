package db.file.writer

import db.file.{FileContextIn, FileContextOut}
import db.util.DatabaseUtils.ShortSize

class ShortHandler extends DataHandler {
  override val Size: Int = ShortSize
  override def write(context: FileContextOut, value: Long): Unit = context.writeShort(value.toInt)
  override def read(context: FileContextIn, offset: Long): Long = context.readShort(offset)
}
