package db

import db.file.FileContext


abstract class DatabaseLevel[Q, R] {

  def query(context: FileContext, offset: Int, limit: Int, parameters: Q): R

  def queryFirst(context: FileContext, parameters: Q): R = query(context, 0, 1, parameters)

  private[db] def empty: R

  def write(context: FileContext, data: Map[Int, Q]): FileContext

}
