package changestream.actors

import akka.actor.{ActorRefFactory, Props}
import akka.testkit.{TestActorRef, TestProbe}
import changestream.actors.PositionSaver.EmitterResult
import changestream.actors.SqsActor.BatchResult
import changestream.helpers.{Config, Emitter}

import scala.concurrent.duration._
import scala.language.postfixOps

class SqsActorSpec extends Emitter with Config {
  val probe = TestProbe()
  val maker = (_: ActorRefFactory) => probe.ref
  val actorRef = TestActorRef(Props(classOf[SqsActor], maker, awsConfig))

  "When SqsActor receives a single valid message" should {
    "Add the message to the SQS queue in a batch of one" in {
      actorRef ! message

      val result = probe.expectMsgType[EmitterResult](5000 milliseconds)
      result.position should be(message.nextPosition)
      result.meta.get shouldBe a[BatchResult]
      result.meta.get.asInstanceOf[BatchResult].failed shouldBe empty
      result.meta.get.asInstanceOf[BatchResult].queued should have length 1
    }
  }

  "When SqsActor receives multiple valid messages in quick succession" should {
    "Add the messages to the SQS queue in a batch of multiple" in {
      actorRef ! message
      actorRef ! message.copy(nextPosition = "FOOBAZ")

      val result = probe.expectMsgType[EmitterResult](5000 milliseconds)
      result.position should be("FOOBAZ")
      result.meta.get shouldBe a[BatchResult]
      result.meta.get.asInstanceOf[BatchResult].failed shouldBe empty
      result.meta.get.asInstanceOf[BatchResult].queued should have length 2
    }
  }

  "When SqsActor receives multiple valid messages in slow succession" should {
    "Add the messages to the SQS queue in multiple batches of one message" in {
      actorRef ! message
      Thread.sleep(500)
      actorRef ! message.copy(nextPosition = "FOOBAZ")

      val result1 = probe.expectMsgType[EmitterResult](5000 milliseconds)
      val result2 = probe.expectMsgType[EmitterResult](5000 milliseconds)

      result1.position should be(message.nextPosition)
      result1.meta.get shouldBe a[BatchResult]
      result1.meta.get.asInstanceOf[BatchResult].failed shouldBe empty
      result1.meta.get.asInstanceOf[BatchResult].queued should have length 1

      result2.position should be("FOOBAZ")
      result2.meta.get shouldBe a[BatchResult]
      result2.meta.get.asInstanceOf[BatchResult].failed shouldBe empty
      result2.meta.get.asInstanceOf[BatchResult].queued should have length 1
    }
  }
}
