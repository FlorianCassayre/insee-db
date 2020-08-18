import java.io.File

import db.InseeDatabaseReader
import org.kohsuke.args4j.{Argument, CmdLineException, CmdLineParser, Option}
import util.CliUtils._

object MainTest extends App {

  object CliArgs {

    @Argument
    var arguments: java.util.List[String] = new java.util.ArrayList[String]()

    @Option(name = "-surname", aliases = Array("-s"), required = true, usage = "Searches with the specified surname(s).")
    var surname: String = _

    @Option(name = "-name", aliases = Array("-n"), usage = "Searches with the specified name(s).")
    var name: String = _

    @Option(name = "-place", aliases = Array("-p"), usage = "Searches at the given place.")
    var place: String = _

    @Option(name = "-event", aliases = Array("-e"), usage = "Filters by the given event type.")
    var event: String = _
    var eventBoolean: Boolean = true

    @Option(name = "-after", aliases = Array("-a"), usage = "Searches for events occurring after or during the specified year.")
    var after: Integer = _

    @Option(name = "-before", aliases = Array("-b"), usage = "Searches for events occurring before or during the specified year.")
    var before: Integer = _

    @Option(name = "-order", aliases = Array("-o"), usage = "Defines the ordering of the results.")
    var order: String = _
    var orderBoolean: Boolean = true

    @Option(name = "-offset", aliases = Array("-k"), usage = "Defines the offset of the result set.")
    var offset: Int = 0

    @Option(name = "-limit", aliases = Array("-l"), usage = "Defines the maximum number of results to be displayed.")
    var limit: Int = 10

    @Option(name = "-stats", aliases = Array("-z"), usage = "Aggregate geographical statistics for this query.")
    var statsMode: Boolean = false

  }

  val parser = new CmdLineParser(CliArgs)
  try {
    import scala.jdk.CollectionConverters._
    parser.parseArgument(args.toList.asJava)

    CliArgs.eventBoolean = scala.Option(CliArgs.event).map(_.trim.toLowerCase) match {
      case None | Some("birth") => true
      case Some("death") => false
      case _ => throw new CmdLineException("Unable to parse event type")
    }

    CliArgs.orderBoolean = scala.Option(CliArgs.order).map(_.trim.toLowerCase) match {
      case None | Some("ascending") => true
      case Some("descending") => false
      case _ => throw new CmdLineException("Unable to parse ordering type")
    }
  } catch {
    case e: CmdLineException =>
      print(s"Error: ${e.getMessage}\n Usage:\n")
      parser.printUsage(System.out)
      System.exit(1)
  }

  val db = new InseeDatabaseReader(new File(CliArgs.arguments.get(0)))

  val t0 = System.currentTimeMillis()

  var t1: Long = _
  if(!CliArgs.statsMode) {
    val place = scala.Option(CliArgs.place).map(v => db.queryPlacesByPrefix(1, v).map(_.id).headOption.getOrElse(-1)).getOrElse(0)

    val result = db.queryPersons(CliArgs.offset, CliArgs.limit, placeId = Some(place), name = scala.Option(CliArgs.name), surname = scala.Option(CliArgs.surname), filterByBirth = CliArgs.eventBoolean, after = if(CliArgs.after == null) None else Some(CliArgs.after), before = if(CliArgs.before == null) None else Some(CliArgs.before), ascending = CliArgs.orderBoolean)

    t1 = System.currentTimeMillis()

    println(s"${result.total} entries (${result.entries.size} shown)")
    println(asciiTable(PersonFieldsHeader, result.entries.map(personToFields)))
  } else {
    val result = db.queryPlaceStatisticsCode(name = scala.Option(CliArgs.name), surname = scala.Option(CliArgs.surname), placeCode = Some("C-FR"))

    t1 = System.currentTimeMillis()

    result.foreach(println)
  }

  println(s"Result in ${t1 - t0} ms")

  db.dispose()

}
