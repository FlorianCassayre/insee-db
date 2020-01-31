package db


abstract class LevelIndex[Q, R, T] extends DatabaseLevel[Q, R] {

  private[db] def getParameter(q: Q): T

}
