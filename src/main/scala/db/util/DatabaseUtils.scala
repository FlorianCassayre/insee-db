package db.util

import db.file.FileContext

object DatabaseUtils {

  val LongSize: Int = java.lang.Long.BYTES
  val IntSize: Int = java.lang.Integer.BYTES
  val ByteSize: Int = java.lang.Byte.BYTES

  val PointerSize: Int = IntSize // TODO add one byte


  def binarySearch(reader: Int => Int, begin: Int, len: Int, step: Int, needle: Int): Option[Int] = {
    var start = 0
    var end = len - 1
    while(start <= end) {
      val mid = Math.floorDiv(start + end, 2)
      val index = begin + mid * step
      val read = reader(index)
      if(read == needle) {
        return Some(index)
      } else if(read < needle) {
        start = mid + 1
      } else {
        end = mid - 1
      }
    }
    return None
  }

}
