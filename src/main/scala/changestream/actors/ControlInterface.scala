package changestream.actors

import akka.util.Timeout
import spray.httpx.SprayJsonSupport._
import spray.routing._
import akka.actor._
import ch.qos.logback.classic.Level
import changestream.{ChangeStream, ChangeStreamEventDeserializer, ChangeStreamEventListener}
import org.slf4j.LoggerFactory
import ch.qos.logback.classic.Logger
import spray.http.StatusCodes
import spray.routing.HttpService
import spray.json.DefaultJsonProtocol

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps

class ControlInterfaceActor extends Actor with ControlInterface {
  def actorRefFactory = context
  def receive = runRoute(controlRoutes)
}

trait ControlInterface extends HttpService with DefaultJsonProtocol {
  import ControlActor._

  protected val log = LoggerFactory.getLogger(getClass)

  // yes this is backward on purpose
  implicit val memoryInfoFormat = jsonFormat3(MemoryInfo)
  implicit val statusFormat = jsonFormat7(Status)
  implicit def executionContext = actorRefFactory.dispatcher
  implicit val timeout = Timeout(10 seconds)

  def controlRoutes: Route = {
    get {
      pathSingleSlash {
        detach() {
          complete(getStatus)
        }
      } ~
      path("status") {
        detach() {
          complete(getStatus)
        }
      } ~
      path("logs") {
        parameter('level) { level => setLogLevel(level) }
      }
    }
  }

  def setLogLevel(level: String) = {
    level.toLowerCase match {
      case "all" | "trace" | "debug" | "info" | "warn" | "error" | "off" =>
        val rootLogger = LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).asInstanceOf[Logger]
        rootLogger.setLevel(Level.toLevel(level))
        complete("ChangeStream logging level has been set to {}.", level)
      case _ =>
        log.error("ControlActor received invalid log level {}.", level)
        complete(StatusCodes.BadRequest, s"Invalid log level: ${level}")
    }
  }

  def getStatus = {
    val storedPosition = Await.result(ChangeStreamEventListener.getStoredPosition, 60 seconds)

    Status(
      server = ChangeStream.serverName,
      clientId = ChangeStream.clientId,
      isConnected = ChangeStream.isConnected,
      binlogClientPosition = ChangeStreamEventListener.getCurrentPosition,
      lastStoredPosition = storedPosition.getOrElse(""),
      binlogClientSequenceNumber = ChangeStreamEventDeserializer.getCurrentSequenceNumber,
      memoryInfo = MemoryInfo(
        Runtime.getRuntime().totalMemory(),
        Runtime.getRuntime().maxMemory(),
        Runtime.getRuntime().freeMemory()
      )
    )
  }
}

object ControlActor {
  case class Status(
                     server: String,
                     clientId: Long,
                     isConnected: Boolean,
                     binlogClientPosition: String,
                     lastStoredPosition: String,
                     binlogClientSequenceNumber: Long,
                     memoryInfo: MemoryInfo
                   )

  case class MemoryInfo(heapSize: Long, maxHeap: Long, freeHeap: Long)
}
