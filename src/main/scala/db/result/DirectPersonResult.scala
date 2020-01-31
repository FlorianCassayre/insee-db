package db.result

import data.Person
import db.file.FileContext

class DirectPersonResult extends DirectMappingResult[Person] {

  // TODO

  override def readResultEntry(context: FileContext): Person = ???

  override def writeResultEntry(context: FileContext, entry: Person): FileContext = ???

}
