package changestream.actors

import akka.actor.{ActorRefFactory, Props}
import akka.testkit.{TestActorRef, TestProbe}
import changestream.actors.PositionSaver.EmitterResult
import changestream.helpers.{Config, Emitter}

import scala.concurrent.duration._
import com.typesafe.config.ConfigFactory

import scala.language.postfixOps

class S3ActorSpec extends Emitter with Config {
  val probe = TestProbe()
  val maker = (_: ActorRefFactory) => probe.ref
  val s3Config = ConfigFactory.
    parseString("aws.s3.batch-size = 2, aws.s3.flush-timeout = 1000").
    withFallback(awsConfig)
  val actorRef = TestActorRef(Props(classOf[S3Actor], maker, s3Config))

  "When S3Actor receives a single valid message" should {
    "Add the message to S3 in a batch of one" in {
      actorRef ! message

      val result = probe.expectMsgType[EmitterResult](5000 milliseconds)
      result.position should be(message.nextPosition)
      result.meta.get.asInstanceOf[String] should endWith ("-1.json")
    }
  }

  "When S3Actor receives multiple valid messages in quick succession" should {
    "Add the messages to S3 in a batch of many" in {
      actorRef ! message
      actorRef ! message.copy(nextPosition = "FOOBAZ")

      val result = probe.expectMsgType[EmitterResult](5000 milliseconds)
      result.position should be("FOOBAZ")
      result.meta.get.asInstanceOf[String] should endWith ("-2.json")
    }
  }

  "When S3Actor receives multiple valid messages in slow succession" should {
    "Add the messages to the S3 queue in multiple batches of one message" in {
      actorRef ! message
      Thread.sleep(2000)
      actorRef ! message.copy(nextPosition = "FOOBAZ")

      val result1 = probe.expectMsgType[EmitterResult](5000 milliseconds)
      val result2 = probe.expectMsgType[EmitterResult](5000 milliseconds)
      result1.position should be(message.nextPosition)
      result1.meta.get.asInstanceOf[String] should endWith ("-1.json")
      result2.position should be("FOOBAZ")
      result2.meta.get.asInstanceOf[String] should endWith ("-1.json")
    }
  }

  "When S3Actor receives multiple valid messages that exceed the flush size" should {
    "Add the messages to the S3 queue in multiple batches" in {
      actorRef ! message
      actorRef ! message.copy(nextPosition = "FOOBAZ")
      actorRef ! message.copy(nextPosition = "BIPBOP")

      val result1 = probe.expectMsgType[EmitterResult](5000 milliseconds)
      val result2 = probe.expectMsgType[EmitterResult](5000 milliseconds)
      result1.position should be("FOOBAZ")
      result1.meta.get.asInstanceOf[String] should endWith ("-2.json")
      result2.position should be("BIPBOP")
      result2.meta.get.asInstanceOf[String] should endWith ("-1.json")
    }
  }
}
