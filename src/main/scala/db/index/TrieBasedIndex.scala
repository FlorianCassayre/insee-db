package db.index

import db.file.writer.DataHandler

trait TrieBasedIndex {

  val childrenCountHandler: DataHandler
  val keyHandler: DataHandler

}
