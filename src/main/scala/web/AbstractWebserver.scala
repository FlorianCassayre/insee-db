package web

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Directives.validate
import akka.http.scaladsl.server.{Directive0, Route}
import akka.http.scaladsl.settings.ServerSettings

import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.io.StdIn

trait AbstractWebserver extends BaseResponseHandler {

  protected implicit val system: ActorSystem = ActorSystem()
  protected implicit val executionContext: ExecutionContextExecutor = system.dispatcher

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

    val bindingFuture: Future[Http.ServerBinding] =
      Http().newServerAt(interface, port)
        .withSettings(ServerSettings.apply(system).withParsingErrorHandler(CustomErrorParsingHandler.getClass.getName))
        .bind(route)

    println(s"Server online at http://$interface:$port\nPress RETURN to stop...")
    StdIn.readLine() // Let it run until user presses return
    bindingFuture
      .flatMap(_.unbind()) // Trigger unbinding from the port
      .onComplete(_ => system.terminate()) // Shutdown when done
  }
}
