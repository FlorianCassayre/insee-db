package db.index

abstract class PrefixIndex[Q, R] extends AbstractFullTrieIndex[Q, R, Seq[Int]] {

  override def canonicalize(seq: Seq[Int]): Seq[Int] = seq

  override def reorder(seq: Seq[Int]): Seq[Seq[Int]] = Seq(seq)

}
