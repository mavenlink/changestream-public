package changestream

import com.github.shyiko.mysql.binlog.BinaryLogClient
import com.github.shyiko.mysql.binlog.BinaryLogClient.LifecycleListener
import org.slf4j.LoggerFactory

object ChangeStreamLifecycleListener extends LifecycleListener {
  protected val log = LoggerFactory.getLogger(getClass)

  def onConnect(client: BinaryLogClient) = {
    log.info("MySQL client connected!")
  }

  def onDisconnect(client: BinaryLogClient) = {
    log.info("MySQL binlog client was disconnected.")
  }

  def onEventDeserializationFailure(client: BinaryLogClient, ex: Exception) = {
    log.error("MySQL client failed to deserialize event.", ex)
    ChangeStreamEventListener.shutdownAndExit(2)
  }

  def onCommunicationFailure(client: BinaryLogClient, ex: Exception) = {
    log.error("MySQL client communication failure.", ex)

    if(ex.isInstanceOf[com.github.shyiko.mysql.binlog.network.ServerException]) {
      // If the server immediately replies with an exception, sleep for a bit
      Thread.sleep(5000)
    }
  }
}
