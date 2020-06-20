package web

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.StatusCodes.{BadRequest, MethodNotAllowed, NotFound}
import akka.http.scaladsl.server.Directives.{complete, validate}
import akka.http.scaladsl.server.{Directive0, MalformedQueryParamRejection, MethodRejection, MissingQueryParamRejection, RejectionHandler, Route, ValidationRejection}
import akka.stream.ActorMaterializer
import spray.json.{DefaultJsonProtocol, RootJsonFormat}

import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.io.StdIn

trait AbstractWebserver extends SprayJsonSupport with DefaultJsonProtocol {

  protected implicit val system: ActorSystem = ActorSystem()
  protected implicit val materializer: ActorMaterializer = ActorMaterializer()
  protected implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  protected val cors: CORSHandler = new CORSHandler {}

  protected abstract class Response {
    val code: Int
  }

  protected case class ErrorResponse(override val code: Int, error: String, message: String) extends Response

  protected implicit val errorResponseFormat: RootJsonFormat[ErrorResponse] = jsonFormat3(ErrorResponse)

  protected implicit def rejectionHandler: RejectionHandler =
    RejectionHandler.newBuilder()
      .handle {
        case MissingQueryParamRejection(param) =>
          val errorResponse = ErrorResponse(BadRequest.intValue, "Missing Parameter", s"The required parameter '$param' was not found.")
          cors.corsHandler(complete(errorResponse.code, errorResponse))
        case MalformedQueryParamRejection(param, _, _) =>
          val errorResponse = ErrorResponse(BadRequest.intValue, "Malformed Parameter", s"The parameter '$param' could not be interpreted.")
          cors.corsHandler(complete(errorResponse.code, errorResponse))
        case ValidationRejection(message, _) =>
          val errorResponse = ErrorResponse(BadRequest.intValue, "Invalid Parameter", message)
          cors.corsHandler(complete(errorResponse.code, errorResponse))
      }
      .handleNotFound {
        val errorResponse = ErrorResponse(NotFound.intValue, "Not Found", "The requested resource could not be found.")
        cors.corsHandler(complete(errorResponse.code, errorResponse))
      }
      .handleAll[MethodRejection] { methodRejections =>
        val names = methodRejections.map(_.supported.name)
        val errorResponse = ErrorResponse(MethodNotAllowed.intValue, "Unsupported Method", s"This method is not supported for this request (allowed: ${names.mkString(", ")}).")
        cors.corsHandler(complete(errorResponse.code, errorResponse))
      }
      .result()

  protected def validateNonEmpty(parameter: String, value: String): Directive0 = {
    validate(value.trim.nonEmpty, s"The parameter '$parameter' cannot be empty.")
  }

  protected def validateNonNegative(parameter: String, n: Int): Directive0 = {
    validate(n >= 0, s"The parameter '$parameter' cannot be negative.")
  }

  protected def validateNotGreater(parameter: String, n: Int, upperBound: Int): Directive0 = {
    validate(n <= upperBound, s"The parameter '$parameter' cannot be greater than $upperBound.")
  }

  protected def validatePositiveBounded(parameter: String, n: Int, upperBound: Int): Directive0 = {
    validateNonNegative(parameter, n) & validateNotGreater(parameter, n, upperBound)
  }

  protected def validateConvert[T](parameter: String, value: String, option: Option[T]): Directive0 = {
    validate(option.nonEmpty, s"Unrecognized value '$value' for parameter '$parameter'.")
  }

  val interface = "localhost"
  val port = 8080

  protected val route: Route // To be defined

  private var started = false
  def bootstrap(): Unit = {
    assert(!started)
    started = true

    val bindingFuture: Future[Http.ServerBinding] = Http().bindAndHandle(route, interface, port)

    println(s"Server online at http://localhost:8080/\nPress RETURN to stop...")
    StdIn.readLine() // Let it run until user presses return
    bindingFuture
      .flatMap(_.unbind()) // Trigger unbinding from the port
      .onComplete(_ => system.terminate()) // Shutdown when done
  }
}
