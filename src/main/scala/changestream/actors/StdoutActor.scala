package changestream.actors

import akka.actor.{Actor, ActorRef, ActorRefFactory}
import changestream.actors.PositionSaver.EmitterResult
import changestream.events.MutationWithInfo
import com.typesafe.config.{Config, ConfigFactory}
import kamon.Kamon

class StdoutActor(getNextHop: ActorRefFactory => ActorRef,
                  config: Config = ConfigFactory.load().getConfig("changestream")) extends Actor {
  protected val nextHop = getNextHop(context)
  protected val counterMetric = Kamon.counter("changestream.emitter.total").withTag("emitter","stdout").withTag("result", "success")

  def receive = {
    case MutationWithInfo(_, pos, _, _, Some(message: String)) =>
      println(message)
      counterMetric.increment()
      nextHop ! EmitterResult(pos)
  }
}
