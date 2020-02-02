package db.index
import db.file.writer.{ByteHandler, DataHandler}

trait StringBasedIndex extends TrieBasedIndex {

  override val childrenCountHandler: DataHandler = new ByteHandler()
  override val keyHandler: DataHandler = new ByteHandler()

}
