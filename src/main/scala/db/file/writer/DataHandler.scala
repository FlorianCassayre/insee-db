package db.file.writer

import db.file.FileContext

abstract class DataHandler {

  val Size: Int
  def write(context: FileContext, offset: Long, value: Long): Unit
  def read(context: FileContext, offset: Long): Long

}