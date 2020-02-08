package db.file.writer

import db.file.{FileContextIn, FileContextOut}
import db.util.DatabaseUtils.ByteSize

class ByteHandler extends DataHandler {
  override val Size: Int = ByteSize
  override def write(context: FileContextOut, value: Long): Unit = context.writeByte(value.toInt)
  override def read(context: FileContextIn, offset: Long): Long = context.readByte(offset)
}
