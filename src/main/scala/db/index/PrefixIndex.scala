package db.index

import collection.Seq

abstract class PrefixIndex[Q, P, R] extends AbstractFullTrieIndex[Q, P, R, Seq[Seq[Int]]] {

  override def canonicalize(seq: Seq[Seq[Int]]): Seq[Int] = seq match { case Seq(singleton) => singleton }

  override def reorder(seq: Seq[Seq[Int]]): Seq[Seq[Int]] = seq

}
