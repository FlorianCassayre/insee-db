import java.io.File

import db.InseeDatabase

object MainWrite extends App {

  require(args.length == 4, "Arguments: <output directory> <insee official files directory> <places directory> <names file>")

  val db = new InseeDatabase(new File(args(0)), readonly = false)

  db.generateDatabase(new File(args(1)), new File(args(2)), new File(args(3)))

  db.dispose()

}
