package db


abstract class LevelIndex[Q, P, R, T] extends DatabaseLevel[Q, P, R] {

  private[db] def getQueryParameter(q: Q): T

  private[db] def getWriteParameter(q: P): T

}
