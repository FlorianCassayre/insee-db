import java.io.File
import java.text.SimpleDateFormat
import java.util.Date

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server._
import akka.stream.ActorMaterializer
import data.{PersonDisplay, PlaceDisplay}
import db.InseeDatabase
import spray.json._
import web.CORSHandler

import scala.collection.Seq
import scala.concurrent.ExecutionContextExecutor
import scala.io.StdIn

object MainWebserver extends App with SprayJsonSupport with DefaultJsonProtocol {

  val db = new InseeDatabase(new File(args(0)))

  implicit val system: ActorSystem = ActorSystem()
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  private val cors = new CORSHandler {}

  sealed abstract class Response {
    val code: Int
  }
  case class ErrorResponse(override val code: Int, error: String, message: String) extends Response
  case class PlacesResponse(override val code: Int, results: Seq[PlaceDisplay]) extends Response
  case class PersonsResponse(override val code: Int, count: Int, results: Seq[PersonDisplay]) extends Response

  implicit val errorResponseFormat: RootJsonFormat[ErrorResponse] = jsonFormat3(ErrorResponse)

  implicit val placeFormat: RootJsonFormat[PlaceDisplay] = jsonFormat2(PlaceDisplay)
  implicit val placesResponseFormat: RootJsonFormat[PlacesResponse] = jsonFormat2(PlacesResponse)

  implicit val personsFormat: RootJsonFormat[PersonDisplay] = jsonFormat7(PersonDisplay)
  implicit val personsResponseFormat: RootJsonFormat[PersonsResponse] = jsonFormat3(PersonsResponse)

  implicit def dateJsonConvertor: JsonFormat[Date] = new JsonFormat[Date] {
    private val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")

    override def read(json: JsValue): Date = throw new SerializationException("Not intended to be read")
    override def write(obj: Date): JsValue = JsString(simpleDateFormat.format(obj))
  }


  implicit def rejectionHandler: RejectionHandler =
    RejectionHandler.newBuilder()
      .handle {
        case MissingQueryParamRejection(param) =>
          val errorResponse = ErrorResponse(BadRequest.intValue, "Missing Parameter", s"The required parameter '$param' was not found.")
          complete(errorResponse.code, errorResponse)
      }
      .handle {
        case MalformedQueryParamRejection(param, _, _) =>
          val errorResponse = ErrorResponse(BadRequest.intValue, "Malformed Parameter", s"The parameter '$param' could not be interpreted.")
          complete(errorResponse.code, errorResponse)
      }
      .handle {
        case ValidationRejection(message, _) =>
          val errorResponse = ErrorResponse(BadRequest.intValue, "Invalid Parameter", message)
          complete(errorResponse.code, errorResponse)
      }
      .handleNotFound {
        val errorResponse = ErrorResponse(NotFound.intValue, "Not Found", "The requested resource could not be found.")
        complete(errorResponse.code, errorResponse)
      }
      .result()

  def validateNonEmpty(parameter: String, value: String): Directive0 = {
    validate(value.trim.nonEmpty, s"The parameter '$parameter' cannot be empty.")
  }

  def validateNonNegative(parameter: String, n: Int): Directive0 = {
    validate(n >= 0, s"The parameter '$parameter' cannot be negative.")
  }

  def validateNotGreater(parameter: String, n: Int, upperBound: Int): Directive0 = {
    validate(n <= upperBound, s"The parameter '$parameter' cannot be greater than $upperBound.")
  }

  def validatePositiveBounded(parameter: String, n: Int, upperBound: Int): Directive0 = {
    validateNonNegative(parameter, n) & validateNotGreater(parameter, n, upperBound)
  }

  def validateConvert[T](parameter: String, value: String, option: Option[T]): Directive0 = {
    validate(option.nonEmpty, s"Unrecognized value '$value' for parameter '$parameter'.")
  }

  val route =
    concat(
      path("places") {
        parameters("prefix".as[String], "limit".as[Int] ? 10) { (prefix, limit) =>
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
        parameters("surname".as[String], "name".as[String] ? "", "place".as[Int] ? 0, "offset".as[Int] ? 0, "limit".as[Int] ? 10,
          "event".as[String] ? keyEventBirth, "after".as[Int].?, "before".as[Int].?, "order".as[String] ? keyOrderAscending) {
          (surname, name, place, offset, limit, event, after, before, order) =>
            val limitMax = 100
            val eventOpt = convertEvent(event)
            val orderOpt = convertOrdering(order)
            (validatePositiveBounded("offset", offset, Int.MaxValue - limitMax) & validatePositiveBounded("limit", limit, limitMax) & validateNonEmpty("surname", surname) & validateConvert("event", event, eventOpt) & validateConvert("order", order, orderOpt)) {
              val (yearMin, yearMax) = (1850, 2019) // TODO refactor this
              def clamp(year: Int): Int = Math.min(Math.max(year, yearMin), yearMax)
              val (realAfter, realBefore) = (after.map(clamp), before.map(clamp))

              val result = db.queryPersons(offset, limit, Some(surname), Some(name), Some(place), filterByBirth = eventOpt.get, realAfter, realBefore, ascending = orderOpt.get)

              val successResponse = PersonsResponse(OK.intValue, result.total, result.entries)
              cors.corsHandler(complete(successResponse.code, successResponse))
            }
        }
      }
    )

  val bindingFuture = Http().bindAndHandle(route, "localhost", 8080)

  println(s"Server online at http://localhost:8080/\nPress RETURN to stop...")
  StdIn.readLine() // let it run until user presses return
  bindingFuture
    .flatMap(_.unbind()) // Trigger unbinding from the port
    .onComplete(_ => system.terminate()) // Shutdown when done

}
