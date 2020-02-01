package db

abstract class LevelIndexParent[Q, P, R, T] extends LevelIndex[Q, P, R, T] {

  private[db] val child: DatabaseLevel[Q, P, R]

  override private[db] def empty: R = child.empty

}