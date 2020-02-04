import java.io.File

import db.InseeDatabase

import util.CliUtils._

object MainTest extends App {

  val db = new InseeDatabase(new File(args(0)))

  val surname = "jean"
  val name = ""

  val result = db.queryPersons(0, 10, placeId = None, name = Some(name), surname = Some(surname))

  println(s"${result.total} entries (${result.entries.size} shown)")
  println(asciiTable(PersonFieldsHeader, result.entries.map(personToFields)))

  db.dispose()

}
