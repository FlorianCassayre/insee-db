package db.file.writer

import db.file.FileContext
import db.util.DatabaseUtils.ByteSize

class ByteHandler extends DataHandler {
  override val Size: Int = ByteSize
  override def write(context: FileContext, offset: Long, value: Long): Unit = context.writeByte(offset, value.toInt)
  override def read(context: FileContext, offset: Long): Long = context.readByte(offset)
}
