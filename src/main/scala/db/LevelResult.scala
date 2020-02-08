package db

import db.file.FileContextIn

abstract class LevelResult[Q, P, R] extends DatabaseLevel[Q, P, R] {

  private[db] def readResult(context: FileContextIn, offset: Int, limit: Int): R

  override def query(context: FileContextIn, offset: Int, limit: Int, parameters: Q): R = readResult(context, offset, limit)

}
