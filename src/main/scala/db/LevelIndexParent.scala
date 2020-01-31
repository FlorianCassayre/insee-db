package db

abstract class LevelIndexParent[Q, R, T] extends LevelIndex[Q, R, T] {

  private[db] val child: DatabaseLevel[Q, R]

  override private[db] def empty: R = child.empty

}