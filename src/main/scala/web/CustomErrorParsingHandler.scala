package web

import akka.event.LoggingAdapter
import akka.http.ParsingErrorHandler
import akka.http.scaladsl.model.{ErrorInfo, HttpResponse, StatusCode}
import akka.http.scaladsl.settings.ServerSettings

object CustomErrorParsingHandler extends ParsingErrorHandler with BaseResponseHandler {
  override def handle(status: StatusCode, info: ErrorInfo, log: LoggingAdapter, settings: ServerSettings): HttpResponse = {
    val errorResponse = ErrorResponse(status.intValue, "Illegal Request", info.formatPretty)
    cors.addCORSHeaders(HttpResponse(status, entity = errorResponse.toJson.compactPrint))
  }
}
