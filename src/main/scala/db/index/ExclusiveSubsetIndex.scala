package db.index

import scala.collection.Seq

abstract class ExclusiveSubsetIndex[Q, P, R] extends AbstractFullTrieIndex[Q, P, R, Seq[Int]] {

  val MaxCombinations = 16

  override def canonicalize(seq: Seq[Int]): Seq[Int] = seq.distinct.sorted

  override def reorder(seq: Seq[Int]): Seq[Seq[Int]] = {
    val sorted = seq.distinct.sorted
    val first = (1 to Math.min(sorted.size, MaxCombinations)).flatMap(n => sorted.combinations(n)).map(_.sorted)
    val prefixes = (1 to sorted.size).map(sorted.take)
    (first ++ prefixes).distinct
  }

}
