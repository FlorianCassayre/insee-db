package db

import db.file.FileContext

abstract class LevelResult[Q, R] extends DatabaseLevel[Q, R] {

  private[db] def readResult(context: FileContext, offset: Int, limit: Int): R

  override def query(context: FileContext, offset: Int, limit: Int, parameters: Q): R = readResult(context, offset, limit)

}
