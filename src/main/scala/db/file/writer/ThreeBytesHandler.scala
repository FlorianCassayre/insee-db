package db.file.writer

import db.file.{FileContextIn, FileContextOut}
import db.util.DatabaseUtils.ByteSize

class ThreeBytesHandler extends DataHandler {
  override val Size: Int = 3 * ByteSize
  override def write(context: FileContextOut, value: Long): Unit = {
    context.writeByte(value.toInt >> 16)
    context.writeShort(value.toInt)
  }
  override def read(context: FileContextIn, offset: Long): Long = {
    val high = context.readByte(offset)
    val low = context.readShort(offset + ByteSize)
    ((high & 0xff) << 16) | (low & 0xffff)
  }
}
