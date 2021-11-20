import data.PlaceDisplay

import java.io.File
import db.InseeDatabaseReader
import org.kohsuke.args4j.{Argument, CmdLineException, CmdLineParser, Option}
import util.CliUtils._

object MainCli extends App {

  object CliArgs {

    @Argument
    var arguments: java.util.List[String] = new java.util.ArrayList[String]()

    @Option(name = "--surname", aliases = Array("-s"), usage = "Searches with the specified surname(s).")
    var surname: String = _

    @Option(name = "--name", aliases = Array("-n"), usage = "Searches with the specified name(s).")
    var name: String = _

    @Option(name = "--place", aliases = Array("-p"), usage = "Searches at the given place.")
    var place: String = _

    @Option(name = "--event", aliases = Array("-e"), usage = "Filters by the given event type.")
    var event: String = _
    var eventBoolean: Boolean = true

    @Option(name = "--after", aliases = Array("-a"), usage = "Searches for events occurring after or during the specified year.")
    var after: Integer = _

    @Option(name = "--before", aliases = Array("-b"), usage = "Searches for events occurring before or during the specified year.")
    var before: Integer = _

    @Option(name = "--order", aliases = Array("-o"), usage = "Defines the ordering of the results.")
    var order: String = _
    var orderBoolean: Boolean = true

    @Option(name = "--offset", aliases = Array("-k"), usage = "Defines the offset of the result set.")
    var offset: Int = 0

    @Option(name = "--limit", aliases = Array("-l"), usage = "Defines the maximum number of results to be displayed.")
    var limit: Int = 10

    @Option(name = "--stats-geography", aliases = Array("-sg"), usage = "Aggregates geographical statistics for this query.", forbids = Array("-st", "-p", "-e", "-a", "-b", "-o", "-k", "-l"))
    var statsGeography: Boolean = false

    @Option(name = "--stats-time", aliases = Array("-st"), usage = "Aggregates temporal statistics for this query.", forbids = Array("-sg", "-a", "-b", "-o", "-k", "-l"))
    var statsTime: Boolean = false

    @Option(name = "--code", aliases = Array("-c"), usage = "Selects the geographical scope for statistical queries.", depends = Array("-z"))
    var statsCode: String = _

    @Option(name = "--autocomplete-place", aliases = Array("-ap"), usage = "Lists the candidate places for autocompletion of a given query string.", forbids = Array("-s", "-n", "-p", "-e", "-a", "-b", "-o", "-k", "-sg", "-st", "-c"))
    var autocompletePlace: String = _

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

    if(CliArgs.surname == null && CliArgs.autocompletePlace == null) {
      throw new CmdLineException("Must specify at least a surname or a place for autocompletion")
    }
  } catch {
    case e: CmdLineException =>
      print(s"Error: ${e.getMessage}\n Usage:\n")
      parser.printUsage(System.out)
      System.exit(1)
  }

  val db = new InseeDatabaseReader(new File(CliArgs.arguments.get(0)))

  val t0 = System.currentTimeMillis()

  lazy val placeOne = scala.Option(CliArgs.place).map(v => db.queryPlacesByPrefix(1, v).map(_.id).headOption.getOrElse(-1)).getOrElse(0)

  var t1: Long = _
  if(CliArgs.surname == null) {
    val result = db.queryPlacesByPrefix(CliArgs.limit, CliArgs.autocompletePlace)

    t1 = System.currentTimeMillis()

    println(asciiTable(PlaceFieldsHeader, result.map { case PlaceDisplay(id, fullname) => Seq(id.toString, fullname) }))
  } else if(!CliArgs.statsGeography && !CliArgs.statsTime) {
    val result = db.queryPersons(CliArgs.offset, CliArgs.limit, placeId = Some(placeOne), name = scala.Option(CliArgs.name), surname = scala.Option(CliArgs.surname), filterByBirth = CliArgs.eventBoolean, after = if(CliArgs.after == null) None else Some(CliArgs.after), before = if(CliArgs.before == null) None else Some(CliArgs.before), ascending = CliArgs.orderBoolean)

    t1 = System.currentTimeMillis()

    println(s"${result.total} entries (${result.entries.size} shown)")
    println(asciiTable(PersonFieldsHeader, result.entries.map(personToFields)))
  } else if(CliArgs.statsGeography) {
    val placeCode = if(CliArgs.statsCode == null || CliArgs.statsCode.trim.isEmpty) None else Some(CliArgs.statsCode)
    val result = db.queryPlaceStatisticsCode(name = scala.Option(CliArgs.name), surname = scala.Option(CliArgs.surname), placeCode = placeCode, nestingDepth = Some(1))

    t1 = System.currentTimeMillis()

    println(asciiTable(StatsGeographyFieldsHeader, result.map { case (code, count) => Seq(code, count.toString) }))
  } else if(CliArgs.statsTime) {
    val result = db.queryTimesStatistics(name = scala.Option(CliArgs.name), surname = scala.Option(CliArgs.surname), placeId = Some(placeOne), filterByBirth = CliArgs.eventBoolean)

    t1 = System.currentTimeMillis()

    println(asciiTable(StatsTimeFieldsHeader, result.map { case (year, count) => Seq(year.toString, count.toString) }))
  }

  println(s"Result in ${t1 - t0} ms")

  db.dispose()

}
