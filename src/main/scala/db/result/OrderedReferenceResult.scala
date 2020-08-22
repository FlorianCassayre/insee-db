package db.result

import db.{LevelResult, ResultSet}

trait OrderedReferenceResult[Q, P, R] extends LevelResult[Q, P, R] {

  val MaxResults: Int = Int.MaxValue

  // Default implementations

  val OrdersCount: Int = 1

  def ordering(i: Int)(id: Int, value: P): Long = id

  def getOrder(q: Q): Int = 0

  def orderTransformer(i: Int)(id: Int): Int = id

}
