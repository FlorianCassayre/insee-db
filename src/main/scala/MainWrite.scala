import java.io.File

import db.{InseeDatabaseReader, InseeDatabaseWriter}

object MainWrite extends App {

  require(Seq(2, 3, 6, 7).contains(args.length), "Arguments: <output directory> (<insee sources> | <insee official files directory> <places directory> <names file> <blacklist file> <wikidata file>) [DRY-RUN number of rows to load]")

  val db = new InseeDatabaseWriter(new File(args(0)))

  val dryRunOption: Option[Int] = {
    if(Seq(3, 7).contains(args.length)) {
      val dryRunRows = args.last.toInt
      println(s"[WARNING] Dry-run mode is enabled for this execution, only the first $dryRunRows persons rows will be loaded")
      Some(dryRunRows)
    } else {
      None
    }
  }

  if(args.length <= 3) {
    db.generateDatabase(new File(args(1)), dryRunOption)
  } else {
    db.generateDatabase(new File(args(1)), new File(args(2)), new File(args(3)), new File(args(4)), new File(args(5)), dryRunOption)
  }

}
