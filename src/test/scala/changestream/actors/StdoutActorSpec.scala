package changestream.actors

import akka.actor.{ActorRefFactory, Props}
import akka.testkit.{TestActorRef, TestProbe}
import changestream.actors.PositionSaver.EmitterResult
import changestream.helpers.{Config, Emitter}

import scala.concurrent.duration._
import scala.language.postfixOps

class StdoutActorSpec extends Emitter with Config {
  val probe = TestProbe()
  val maker = (_: ActorRefFactory) => probe.ref
  val actorRef = TestActorRef(Props(classOf[StdoutActor], maker, awsConfig))

  "When StdoutActor receives a single valid message" should {
    "Print to stdout and forward result" in {
      actorRef ! message

      val result = probe.expectMsgType[EmitterResult](5000 milliseconds)
      result.position should be(message.nextPosition)
    }
  }
}
