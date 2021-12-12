import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.server.Directives.complete
import akka.http.scaladsl.server.Route
import spray.json.RootJsonFormat
import web.AbstractWebserver

object MainWebserverOffline extends App with AbstractWebserver {

  case class OfflineResponse(override val code: Int, information: Option[String]) extends Response

  protected implicit val offlineResponseFormat: RootJsonFormat[OfflineResponse] = jsonFormat2(OfflineResponse)

  private val message = args.headOption

  println("Starting server in offline mode...")
  if(message.isEmpty) {
    println("Note: no default message was defined")
  } else {
    println(s"The following message will be displayed to visitors: '${message.get}'")
  }

  override protected val route: Route = Route.seal {
    val offlineResponse = OfflineResponse(ServiceUnavailable.intValue, message)
    cors.corsHandler(complete(offlineResponse.code, offlineResponse))
  }

  bootstrap()
}
