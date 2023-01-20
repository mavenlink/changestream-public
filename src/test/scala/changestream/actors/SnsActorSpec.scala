package changestream.actors

import akka.actor.{ActorRefFactory, Props}
import akka.testkit.{TestActorRef, TestProbe}
import changestream.actors.PositionSaver.EmitterResult
import changestream.helpers.{Config, Emitter}
import com.typesafe.config.ConfigFactory

import scala.concurrent.duration._
import scala.language.postfixOps

class SnsActorSpec extends Emitter with Config {
  val probe = TestProbe()
  val maker = (_: ActorRefFactory) => probe.ref

  val actorRef = TestActorRef(Props(classOf[SnsActor], maker, awsConfig))

  val configWithInterpolation = ConfigFactory.
    parseString("aws.sns.topic = \"__integration_tests-{database}-{tableName}\"").
    withFallback(awsConfig)
  val snsWithInterpolation = TestActorRef(Props(classOf[SnsActor], maker, configWithInterpolation))


  "When SnsActor receives a single valid message" should {
    "Immediately publish the message to SNS" in {
      actorRef ! message

      val result = probe.expectMsgType[EmitterResult](5000 milliseconds)
      result.position should be(message.nextPosition)
    }
  }

  "When SnsActor receives a message" should {
    "Should correctly publish the message when the topic contains interpolated database and/or tableName" in {
      snsWithInterpolation ! message

      val result = probe.expectMsgType[EmitterResult](5000 milliseconds)
      result.position should be(message.nextPosition)
    }
  }
}
