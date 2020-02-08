package db

import db.file.{FileContextIn, FileContextOut}

import collection._

abstract class DatabaseLevel[Q, P, R] {

  def query(context: FileContextIn, offset: Int, limit: Int, parameters: Q): R

  def queryFirst(context: FileContextIn, parameters: Q): R = query(context, 0, 1, parameters)

  private[db] def empty: R

  def write(context: FileContextOut, data: Map[Int, P]): Unit

}
