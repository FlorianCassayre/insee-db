package web

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.{ContentTypes, EntityStreamSizeException, ExceptionWithErrorInfo, HttpEntity, HttpResponse, IllegalRequestException}
import akka.http.scaladsl.model.StatusCodes.{BadRequest, InternalServerError, MethodNotAllowed, NotFound, PayloadTooLarge}
import akka.http.scaladsl.server.Directives.complete
import akka.http.scaladsl.server.{ExceptionHandler, MalformedQueryParamRejection, MethodRejection, MissingQueryParamRejection, RejectionHandler, ValidationRejection}
import spray.json.{DefaultJsonProtocol, RootJsonFormat}

import scala.util.control.NonFatal

trait BaseResponseHandler extends SprayJsonSupport with DefaultJsonProtocol {

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
      .withFallback( // https://doc.akka.io/docs/akka-http/current/routing-dsl/rejections.html
        RejectionHandler.default
          .mapRejectionResponse {
            case res @ HttpResponse(_, _, ent: HttpEntity.Strict, _) =>
              val message = ent.data.utf8String
              val errorResponse = ErrorResponse(res.status.intValue(), "Rejection", message)
              cors.addCORSHeaders(res.withEntity(HttpEntity(ContentTypes.`application/json`, errorResponse.toJson.convertTo[String])))
            case x => x // Fallback
          }
      )

  protected implicit def exceptionHandler: ExceptionHandler =
    ExceptionHandler {
      case IllegalRequestException(info, status) =>
        val errorResponse = ErrorResponse(status.intValue, "Illegal Request", info.formatPretty)
        cors.corsHandler(complete(errorResponse.code, errorResponse))
      case e: EntityStreamSizeException =>
        val errorResponse = ErrorResponse(PayloadTooLarge.intValue, "Payload Too Large", e.getMessage)
        cors.corsHandler(complete(errorResponse.code, errorResponse))
      case e: ExceptionWithErrorInfo =>
        val errorResponse = ErrorResponse(InternalServerError.intValue, "Internal Server Error", e.info.formatPretty)
        cors.corsHandler(complete(errorResponse.code, errorResponse))
      case NonFatal(_) =>
        val errorResponse = ErrorResponse(InternalServerError.intValue, "Internal Server Error", "(no message supplied)")
        cors.corsHandler(complete(errorResponse.code, errorResponse))
    }

}
