import java.io.File

import db.InseeDatabase

import CliUtils._

object Main extends App {

  val db = new InseeDatabase(new File(args(0)), readonly = false)

  val surname = "jean"
  val name = ""

  val result = db.queryPersons(0, 10, placeId = None, name = Some(name), surname = Some(surname))

  println(s"${result.total} entries (${result.entries.size} shown)")
  println(asciiTable(PersonFieldsHeader, result.entries.map(personToFields)))

  db.dispose()

}
