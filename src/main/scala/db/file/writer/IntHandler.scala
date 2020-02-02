package db.file.writer

import db.file.FileContext
import db.util.DatabaseUtils.IntSize

class IntHandler extends DataHandler {
  override val Size: Int = IntSize
  override def write(context: FileContext, offset: Int, value: Long): Unit = context.writeInt(offset, value.toInt)
  override def read(context: FileContext, offset: Int): Long = context.readInt(offset)
}
