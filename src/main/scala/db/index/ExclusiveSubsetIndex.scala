package db.index

abstract class ExclusiveSubsetIndex[Q, R] extends AbstractFullTrieIndex[Q, R, collection.Set[Int]] {

  override def canonicalize(set: collection.Set[Int]): Seq[Int] = set.toSeq.sorted

  override def reorder(set: collection.Set[Int]): Seq[Seq[Int]] = {
    val seq = set.toSeq
    (1 to seq.size).flatMap(n => seq.combinations(n)).map(_.sorted)
  }

}
