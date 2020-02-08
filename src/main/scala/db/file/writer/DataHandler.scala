package db.file.writer

import db.file.{FileContextIn, FileContextOut}

abstract class DataHandler {

  val Size: Int
  def write(context: FileContextOut, value: Long): Unit
  def read(context: FileContextIn, offset: Long): Long

}
