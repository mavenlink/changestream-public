package changestream

import java.io.IOException
import java.util.concurrent.TimeoutException

import com.github.shyiko.mysql.binlog.BinaryLogClient
import com.typesafe.config.ConfigFactory
import org.slf4j.LoggerFactory

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.concurrent.ExecutionContext.Implicits.global

object ChangeStream extends App {
  protected val log = LoggerFactory.getLogger(getClass)
  protected val config = ConfigFactory.load().getConfig("changestream")
  protected val mysqlHost = config.getString("mysql.host")
  protected val mysqlPort = config.getInt("mysql.port")
  protected val overridePosition = System.getenv("OVERRIDE_POSITION") match {
    case position:String if (position != null && position.length > 0) => Some(position) //scalastyle:ignore
    case _ => None
  }
  protected val client = new BinaryLogClient(
    mysqlHost,
    mysqlPort,
    config.getString("mysql.user"),
    config.getString("mysql.password")
  )

  /** Every changestream instance must have a unique server-id.
    * http://dev.mysql.com/doc/refman/5.7/en/replication-setup-slaves.html#replication-howto-slavebaseconfig
    */
  client.setServerId(config.getLong("mysql.server-id"))

  /** If we lose the connection to the server retry every `changestream.mysql.keepalive` milliseconds. **/
  client.setKeepAliveInterval(config.getLong("mysql.keepalive"))

  ChangeStreamEventListener.setConfig(config)
  ChangestreamEventDeserializerConfig.setConfig(config)

  ChangeStreamEventListener.startControlServer(config)

  client.registerEventListener(ChangeStreamEventListener)
  client.setEventDeserializer(ChangeStreamEventDeserializer)
  client.registerLifecycleListener(ChangeStreamLifecycleListener)

  getConnected(overridePosition)

  def serverName = s"${mysqlHost}:${mysqlPort}"
  def clientId = client.getServerId
  def isConnected = client.isConnected

  def getConnectedAndWait(startingPosition: Option[String]) = Await.result(getConnected(startingPosition), 60.seconds)
  def disconnectClient = client.disconnect()

  def getConnected(startingPosition: Option[String]) = {
    log.info("Starting changestream...")

    val getPositionFuture = startingPosition match {
      case Some(_) =>
        log.info("Overriding starting binlog position with OVERRIDE_POSITION={}", overridePosition)
        ChangeStreamEventListener.setPosition(startingPosition)
      case _ =>
        ChangeStreamEventListener.getStoredPosition
    }

    getPositionFuture.map { position =>
      setBinlogClientPosition(position)
      getInternalClientConnected
    }
  }

  protected def setBinlogClientPosition(position: Option[String]) = position match {
    case Some(position) =>
      log.info("Setting starting binlog position at {}.", position)
      val Array(fileName, posLong) = position.split(":")
      client.setBinlogFilename(fileName)
      client.setBinlogPosition(java.lang.Long.valueOf(posLong))
    case None =>
      log.info("Starting binlog position in real time")
      client.setBinlogFilename(null) //scalastyle:ignore
      client.setBinlogPosition(4L)
  }

  protected def getInternalClientConnected = {
    while(!client.isConnected) {
      try {
        client.connect(5000)
      }
      catch {
        case e: IOException =>
          log.error("Failed to connect to MySQL to stream the binlog, retrying in 5 seconds...", e)
          Thread.sleep(5000)
        case e: TimeoutException =>
          log.error("Timed out connecting to MySQL to stream the binlog, retrying in 5 seconds...", e)
          Thread.sleep(5000)
        case e: Exception =>
          log.error("Failed to connect, exiting...", e)
          Await.result(ChangeStreamEventListener.shutdownAndExit(1), 60.seconds)
      }
    }
  }
}
