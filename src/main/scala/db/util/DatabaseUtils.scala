package db.util

object DatabaseUtils {

  val LongSize: Int = java.lang.Long.BYTES
  val IntSize: Int = java.lang.Integer.BYTES
  val ShortSize: Int = java.lang.Short.BYTES
  val ByteSize: Int = java.lang.Byte.BYTES

  val PointerSize: Int = IntSize + ByteSize // Address space: 2^40 (~ 1 TB)


  def binarySearch(reader: Long => Long, begin: Long, len: Int, step: Int, needle: Int): Option[Long] = {
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
    None
  }

  def binarySearchUpperBound(reader: Long => Long, transform: Int => Int, begin: Long, len: Int, step: Int, upper: Int, callbackOpt: Option[(Long, Int) => Unit] = None): Option[Long] = {
    var start = 0
    var end = len - 1
    var ans: Option[Long] = None
    while(start <= end) {
      val mid = Math.floorDiv(start + end, 2)
      val index = begin + mid * step
      val read = transform(reader(index).toInt)
      callbackOpt.foreach(_(index, read))
      if(read <= upper) {
        ans = Some(mid)
        start = mid + 1
      } else {
        end = mid - 1
      }
    }
    ans
  }

  def binarySearchLowerBound(reader: Long => Long, transform: Int => Int, begin: Long, len: Int, step: Int, lower: Int): Option[Long] = {
    var start = 0
    var end = len - 1
    var ans: Option[Long] = None
    while(start <= end) {
      val mid = Math.floorDiv(start + end, 2)
      val index = begin + mid * step
      val read = transform(reader(index).toInt)
      if(read >= lower) {
        ans = Some(mid)
        end = mid - 1
      } else {
        start = mid + 1
      }
    }
    ans
  }

}
