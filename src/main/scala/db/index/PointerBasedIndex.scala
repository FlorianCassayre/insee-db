package db.index

import db.file.writer.{ByteHandler, DataHandler, IntHandler}

trait PointerBasedIndex extends TrieBasedIndex {

  override val childrenCountHandler: DataHandler = new IntHandler()
  override val keyHandler: DataHandler = new IntHandler()

}
