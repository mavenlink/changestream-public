package changestream

import akka.actor.SupervisorStrategy.{Escalate, Restart}
import akka.actor.{OneForOneStrategy, SupervisorStrategy, SupervisorStrategyConfigurator}
import com.github.mauricio.async.db.mysql.exceptions.MySQLException
import scala.language.postfixOps
import scala.concurrent.duration._

final class ChangestreamSupervisorStrategy extends SupervisorStrategyConfigurator {
  override def create(): SupervisorStrategy = OneForOneStrategy(loggingEnabled = true, maxNrOfRetries = 30, withinTimeRange = 1 minute) {
    case _: MySQLException => Restart
    case _: Exception => Escalate
  }
}
