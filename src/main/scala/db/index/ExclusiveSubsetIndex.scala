package db.index

import collection.{Seq, Set}

abstract class ExclusiveSubsetIndex[Q, P, R] extends AbstractFullTrieIndex[Q, P, R, Set[Int]] {

  override def canonicalize(set: Set[Int]): Seq[Int] = set.toSeq.sorted

  override def reorder(set: Set[Int]): Seq[Seq[Int]] = {
    val seq = set.toSeq
    (1 to seq.size).flatMap(n => seq.combinations(n)).map(_.sorted)
  }

}
