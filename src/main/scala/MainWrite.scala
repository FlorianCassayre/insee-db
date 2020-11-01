import java.io.File

import db.{InseeDatabaseReader, InseeDatabaseWriter}

object MainWrite extends App {

  require(args.length == 2 || args.length == 4, "Arguments: <output directory> (<insee sources> | <insee official files directory> <places directory> <names file>)")

  val db = new InseeDatabaseWriter(new File(args(0)))

  if(args.length == 2) {
    db.generateDatabase(new File(args(1)))
  } else {
    db.generateDatabase(new File(args(1)), new File(args(2)), new File(args(3)))
  }

}
