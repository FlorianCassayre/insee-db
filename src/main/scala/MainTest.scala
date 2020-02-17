import java.io.File

import db.InseeDatabase

import util.CliUtils._

object MainTest extends App {

  val db = new InseeDatabase(new File(args(0)))

  val surname = "jean"
  val name = ""
  val filterByBirth = true
  val after = Some(1917)
  val before = Some(2000)
  val ascending = true

  val t0 = System.currentTimeMillis()

  val result = db.queryPersons(0, 10, placeId = None, name = Some(name), surname = Some(surname), filterByBirth = filterByBirth, after = after, before = before, ascending = ascending)

  val t1 = System.currentTimeMillis()

  println(s"${result.total} entries (${result.entries.size} shown)")
  println(asciiTable(PersonFieldsHeader, result.entries.map(personToFields)))

  println(s"Result in ${t1 - t0} ms")

  db.dispose()

}
