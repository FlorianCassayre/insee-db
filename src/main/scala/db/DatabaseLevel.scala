package db

import db.file.FileContext

import collection._

abstract class DatabaseLevel[Q, P, R] {

  def query(context: FileContext, offset: Int, limit: Int, parameters: Q): R

  def queryFirst(context: FileContext, parameters: Q): R = query(context, 0, 1, parameters)

  private[db] def empty: R

  def write(context: FileContext, data: Seq[(Int, P)]): FileContext

}
