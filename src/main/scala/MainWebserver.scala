import java.io.File
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server._
import data.{PersonDisplay, PlaceDisplay}
import db.ParallelInseeDatabase
import spray.json._
import static.Geography
import web.AbstractWebserver

import scala.collection.Seq

object MainWebserver extends App with AbstractWebserver {

  val db = new ParallelInseeDatabase(new File(args(0)))

  case class NamedCount(name: String, count: Int)

  case class PlacesResponse(override val code: Int, results: Seq[PlaceDisplay]) extends Response
  case class PersonsResponse(override val code: Int, count: Int, results: Seq[PersonDisplay]) extends Response
  case class StatsGeographyResponse(override val code: Int, results: Seq[NamedCount]) extends Response
  case class StatsTimeResponse(override val code: Int, results: Seq[NamedCount]) extends Response

  implicit val placeFormat: RootJsonFormat[PlaceDisplay] = jsonFormat2(PlaceDisplay)
  implicit val placesResponseFormat: RootJsonFormat[PlacesResponse] = jsonFormat2(PlacesResponse)

  implicit val personsFormat: RootJsonFormat[PersonDisplay] = jsonFormat9(PersonDisplay)
  implicit val personsResponseFormat: RootJsonFormat[PersonsResponse] = jsonFormat3(PersonsResponse)

  implicit val namedCountFormat: RootJsonFormat[NamedCount] = jsonFormat2(NamedCount)

  implicit val statsGeographyResponseFormat: RootJsonFormat[StatsGeographyResponse] = jsonFormat2(StatsGeographyResponse)

  implicit val statsTimeResponseFormat: RootJsonFormat[StatsTimeResponse] = jsonFormat2(StatsTimeResponse)

  implicit def dateJsonConvertor: JsonFormat[LocalDate] = new JsonFormat[LocalDate] {
    private val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")

    override def read(json: JsValue): LocalDate = throw new SerializationException("Not intended to be read")
    override def write(obj: LocalDate): JsValue = JsString(obj.format(formatter))
  }

  // TODO change these to proper unmarshallers
  val (keyEventBirth, keyEventDeath) = ("birth", "death")
  def convertEvent(str: String): Option[Boolean] = str match {
    case `keyEventBirth` => Some(true)
    case `keyEventDeath` => Some(false)
    case _ => None
  }
  val (keyOrderAscending, keyOrderDescending) = ("ascending", "descending")
  def convertOrdering(str: String): Option[Boolean] = str match {
    case `keyOrderAscending` => Some(true)
    case `keyOrderDescending` => Some(false)
    case _ => None
  }

  override val route: Route =
    Route.seal(
      get { // All methods are GET
        concat( // All methods are located at the root
          path("places") {
            parameters("prefix".as[String], "limit".as[Int] ? 10, "batch".as[Boolean] ? false) { (prefix, limit, _) =>
              validatePositiveBounded("limit", limit, 25) {
                validateNonEmpty("prefix", prefix) {
                  val result = db.queryPlacesByPrefix(limit, prefix)

                  val successResponse = PlacesResponse(OK.intValue, result)
                  cors.corsHandler(complete(successResponse.code, successResponse))
                }
              }
            }
          },
          path("persons") {
            parameters("surname".as[String], "name".as[String] ? "", "place".as[Int] ? 0, "offset".as[Int] ? 0, "limit".as[Int] ? 10,
              "event".as[String] ? keyEventBirth, "after".as[Int].?, "before".as[Int].?, "order".as[String] ? keyOrderAscending, "batch".as[Boolean] ? false) {
              (surname, name, place, offset, limit, event, after, before, order, _) =>
                val limitMax = 100
                val eventOpt = convertEvent(event)
                val orderOpt = convertOrdering(order)
                (validatePositiveBounded("offset", offset, Int.MaxValue - limitMax) & validatePositiveBounded("limit", limit, limitMax) & validateNonEmpty("surname", surname) & validateConvert("event", event, eventOpt) & validateConvert("order", order, orderOpt)) {
                  val (yearMin, yearMax) = (1850, 2023) // TODO refactor this
                  def clamp(year: Int): Int = Math.min(Math.max(year, yearMin), yearMax)
                  val (realAfter, realBefore) = (after.map(clamp), before.map(clamp))

                  val result = db.queryPersons(offset, limit, Some(surname), Some(name), Some(place), filterByBirth = eventOpt.get, realAfter, realBefore, ascending = orderOpt.get)

                  val successResponse = PersonsResponse(OK.intValue, result.total, result.entries)
                  cors.corsHandler(complete(successResponse.code, successResponse))
                }
            }
          },
          pathPrefix("stats") {
            concat(
              path("geography") {
                parameters("surname".as[String], "name".as[String] ? "") {
                  (surname, name) =>
                    val result = db.getInstance().queryPlaceStatisticsCode(surname = Some(surname), name = Some(name), placeCode = Some(Geography.CodeFrance), nestingDepth = Some(2))

                    val successResponse = StatsGeographyResponse(OK.intValue, result.map { case (name, count) => NamedCount(name, count) })
                    cors.corsHandler(complete(successResponse.code, successResponse))
                }
              },
              path("time") {
                parameters("surname".as[String], "name".as[String] ? "", "place".as[Int] ? 0, "event".as[String] ? keyEventBirth) {
                  (surname, name, place, event) =>
                    val eventOpt = convertEvent(event)
                    validateConvert("event", event, eventOpt) {
                      val result = db.getInstance().queryTimesStatistics(surname = Some(surname), name = Some(name), placeId = Some(place), filterByBirth = eventOpt.get)

                      val successResponse = StatsTimeResponse(OK.intValue, result.map { case (year, count) => NamedCount(year.toString, count) })
                      cors.corsHandler(complete(successResponse.code, successResponse))
                    }
                }
              }
            )

          }
        )
      }
    )

  bootstrap()
}
