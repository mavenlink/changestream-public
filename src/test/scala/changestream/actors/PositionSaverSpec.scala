package changestream.actors

import java.io.File

import akka.actor.Props
import akka.testkit.TestActorRef
import akka.pattern.ask
import changestream.actors.PositionSaver._
import changestream.helpers.{Config, Emitter}
import com.typesafe.config.ConfigFactory

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps

class PositionSaverSpec extends Emitter with Config {
  val tempFile = File.createTempFile("positionSaverSpec", "")

  val saverConfig = ConfigFactory.
    parseString(s"position-saver.file-path = ${tempFile.getPath}").
    withFallback(awsConfig)

  before {
    tempFile.delete()
  }

  after {
    while(msgAvailable) { receiveOne(1000.milliseconds) }
    tempFile.delete()
  }

  "When requesting the current position" should {
    "Respond with current position" in {
      val saver = TestActorRef(Props(new PositionSaver(saverConfig)))
      saver ! GetPositionRequest
      expectMsgType[GetPositionResponse](5000 milliseconds)
    }

    "Throw an exception when the save file location can't be read" in {
      val badConfig = ConfigFactory.
        parseString(s"position-saver.file-path = /").
        withFallback(awsConfig)
      val saver = TestActorRef(Props(new PositionSaver(badConfig)))

      saver ! GetPositionRequest
      expectNoMessage(1000 milliseconds)
    }
  }

  "When requesting the last saved position" should {
    "Return the last saved position" in {
      val configWithTwoMaxRecord = ConfigFactory.
        parseString(s"position-saver.max-records = 2\nposition-saver.max-wait = 1000000").
        withFallback(saverConfig)
      val saver = TestActorRef(Props(new PositionSaver(configWithTwoMaxRecord)))

      saver ! SavePositionRequest(Some("last-saved-position"))
      expectMsgType[akka.actor.Status.Success](5000 milliseconds)

      saver ! EmitterResult("new-position")

      saver ! GetLastSavedPositionRequest
      val lastSavedPosition = expectMsgType[GetPositionResponse](5000 milliseconds)
      lastSavedPosition.position should be(Some("last-saved-position"))
    }
  }

  "When restoring last saved position" should {
    "Restore last saved position and return it" in {
      val configWithTwoMaxRecord = ConfigFactory.
        parseString(s"position-saver.max-records = 2\nposition-saver.max-wait = 1000000").
        withFallback(saverConfig)
      val saver = TestActorRef(Props(new PositionSaver(configWithTwoMaxRecord)))

      saver ! SavePositionRequest(Some("last-saved-position"))
      expectMsgType[akka.actor.Status.Success](5000 milliseconds)

      saver ! EmitterResult("new-position")

      saver ! RestoreLastSavedPositionRequest
      val lastSavedPosition = expectMsgType[GetPositionResponse](5000 milliseconds)
      lastSavedPosition.position should be(Some("last-saved-position"))
    }
  }

  "When setting the current position via override" should {
    "Immediately store the override position in memory" in {
      val saver = TestActorRef(Props(new PositionSaver(saverConfig)))

      saver ! GetPositionRequest
      val initialPosition = expectMsgType[GetPositionResponse](5000 milliseconds)
      initialPosition.position should be(None)

      saver ! SavePositionRequest(Some("position"))
      expectMsgType[akka.actor.Status.Success](5000 milliseconds)

      saver ! GetPositionRequest
      val savedPosition = expectMsgType[GetPositionResponse](5000 milliseconds)
      savedPosition.position should be(Some("position"))
    }

    "Immediately persist the override position to the temp file" in {
      val saver = TestActorRef(Props(new PositionSaver(saverConfig)))

      saver ! GetPositionRequest
      val initialPosition = expectMsgType[GetPositionResponse](5000 milliseconds)
      initialPosition.position should be(None)

      saver ! SavePositionRequest(Some("position"))
      expectMsgType[akka.actor.Status.Success](5000 milliseconds)

      val saverReloaded = TestActorRef(Props(new PositionSaver(saverConfig)))

      saverReloaded ! GetPositionRequest
      val savedPosition = expectMsgType[GetPositionResponse](5000 milliseconds)
      savedPosition.position should be(Some("position"))
    }

    "Immediately persist the override position to the temp file when the override position is none" in {
      val saver = TestActorRef(Props(new PositionSaver(saverConfig)))

      saver ! SavePositionRequest(Some("position"))
      expectMsgType[akka.actor.Status.Success](5000 milliseconds)

      saver ! GetPositionRequest
      expectMsgType[GetPositionResponse](5000 milliseconds).position should be(Some("position"))

      saver ! SavePositionRequest(None)
      expectMsgType[akka.actor.Status.Success](5000 milliseconds)

      saver ! GetPositionRequest
      expectMsgType[GetPositionResponse](5000 milliseconds).position should be(None)
    }
  }

  "Save the current position when requested" in {
    val configWithTwoMaxRecord = ConfigFactory.
      parseString(s"position-saver.max-records = 2\nposition-saver.max-wait = 1000000").
      withFallback(saverConfig)
    val saver = TestActorRef(Props(new PositionSaver(configWithTwoMaxRecord)))

    saver ! SavePositionRequest(Some("initial-position"))
    expectMsgType[akka.actor.Status.Success](5000 milliseconds)

    saver ! EmitterResult("new-position")

    saver ! GetPositionRequest
    expectMsgType[GetPositionResponse](5000 milliseconds).position should be(Some("new-position"))

    saver ! GetLastSavedPositionRequest
    expectMsgType[GetPositionResponse](5000 milliseconds).position should be(Some("initial-position"))

    saver ! SaveCurrentPositionRequest
    expectMsgType[akka.actor.Status.Success](5000 milliseconds)

    saver ! GetLastSavedPositionRequest
    expectMsgType[GetPositionResponse](5000 milliseconds).position should be(Some("new-position"))
  }

  "When receiving a mutation" should {
    "Immediately persist the current position in memory" in {
      val saver = TestActorRef(Props(new PositionSaver(saverConfig)))

      saver ! GetPositionRequest
      val initialPosition = expectMsgType[GetPositionResponse](5000 milliseconds)
      initialPosition.position should be(None)

      saver ! EmitterResult("position")

      saver ! GetPositionRequest
      val savedPosition = expectMsgType[GetPositionResponse](5000 milliseconds)
      savedPosition.position should be(Some("position"))

      expectMsgType[akka.actor.Status.Success](5000 milliseconds)
    }

    "Persist the position when MAX_RECORDS has been reached" in {
      val configWithOneMaxRecord = ConfigFactory.
        parseString(s"position-saver.max-records = 1\nposition-saver.max-wait = 1000000").
        withFallback(saverConfig)
      val saverMaxRecordsOne = TestActorRef(Props(new PositionSaver(configWithOneMaxRecord)))

      saverMaxRecordsOne ! GetPositionRequest
      val initialPosition = expectMsgType[GetPositionResponse](5000 milliseconds)
      initialPosition.position should be(None)

      saverMaxRecordsOne ! EmitterResult("position")
      expectMsgType[akka.actor.Status.Success](5000 milliseconds)

      val saverReloaded = TestActorRef(Props(new PositionSaver(configWithOneMaxRecord)))

      saverReloaded ! GetPositionRequest
      val savedPosition = expectMsgType[GetPositionResponse](5000 milliseconds)
      savedPosition.position should be(Some("position"))
    }

    "Don't persist the position when MAX_RECORDS is 0 (disabled)" in {
      val configWithOneMaxRecord = ConfigFactory.
        parseString(s"position-saver.max-records = 0\nposition-saver.max-wait = 1000000").
        withFallback(saverConfig)
      val saverMaxRecordsOne = TestActorRef(Props(new PositionSaver(configWithOneMaxRecord)))

      saverMaxRecordsOne ! GetPositionRequest
      val initialPosition = expectMsgType[GetPositionResponse](5000 milliseconds)
      initialPosition.position should be(None)

      saverMaxRecordsOne ! EmitterResult("position")

      val saverReloaded = TestActorRef(Props(new PositionSaver(configWithOneMaxRecord)))

      saverReloaded ! GetPositionRequest
      val savedPosition = expectMsgType[GetPositionResponse](5000 milliseconds)
      savedPosition.position should be(None)
    }

    "Persist the position when MAX_RECORDS is reached twice (resetting the internal counter)" in {
      val configWithOneMaxRecord = ConfigFactory.
        parseString(s"position-saver.max-records = 1\nposition-saver.max-wait = 1000000").
        withFallback(saverConfig)
      val saverMaxRecordsOne = TestActorRef(Props(new PositionSaver(configWithOneMaxRecord)))

      saverMaxRecordsOne ! GetPositionRequest
      val initialPosition = expectMsgType[GetPositionResponse](5000 milliseconds)
      initialPosition.position should be(None)

      saverMaxRecordsOne ! EmitterResult("position")
      saverMaxRecordsOne ! EmitterResult("position2")
      expectMsgType[akka.actor.Status.Success](5000 milliseconds)
      expectMsgType[akka.actor.Status.Success](5000 milliseconds)

      val saverReloaded = TestActorRef(Props(new PositionSaver(configWithOneMaxRecord)))

      saverReloaded ! GetPositionRequest
      val savedPosition = expectMsgType[GetPositionResponse](5000 milliseconds)
      savedPosition.position should be(Some("position2"))
    }

    "When MAX_RECORDS has not been reached" should {
      "Don't persist the position when MAX_WAIT is set to 0 (disabled)" in {
        val configWithOneMaxRecord = ConfigFactory.
          parseString(s"position-saver.max-records = 2\nposition-saver.max-wait = 0").
          withFallback(saverConfig)
        val saverMaxRecordsOne = TestActorRef(Props(new PositionSaver(configWithOneMaxRecord)))

        saverMaxRecordsOne ! GetPositionRequest
        val initialPosition = expectMsgType[GetPositionResponse](5000 milliseconds)
        initialPosition.position should be(None)

        saverMaxRecordsOne ! EmitterResult("position")

        val saverReloaded = TestActorRef(Props(new PositionSaver(configWithOneMaxRecord)))

        saverReloaded ! GetPositionRequest
        val savedPosition = expectMsgType[GetPositionResponse](5000 milliseconds)
        savedPosition.position should be(None)
      }

      "Persist the position when timeout has expired" in {
        val configWithOneMaxRecord = ConfigFactory.
          parseString(s"position-saver.max-records = 2\nposition-saver.max-wait = 10").
          withFallback(saverConfig)
        val saverMaxRecordsOne = TestActorRef(Props(new PositionSaver(configWithOneMaxRecord)))

        saverMaxRecordsOne ! GetPositionRequest
        val initialPosition = expectMsgType[GetPositionResponse](5000 milliseconds)
        initialPosition.position should be(None)

        Await.result(saverMaxRecordsOne ? EmitterResult("position"), 5000 milliseconds)

        val saverReloaded = TestActorRef(Props(new PositionSaver(configWithOneMaxRecord)))

        saverReloaded ! GetPositionRequest
        val savedPosition = expectMsgType[GetPositionResponse](5000 milliseconds)
        savedPosition.position should be(Some("position"))
      }

      "Don't persist the position when the timeout has not been reached" in {
        val configWithOneMaxRecord = ConfigFactory.
          parseString(s"position-saver.max-records = 2\nposition-saver.max-wait = 100000").
          withFallback(saverConfig)
        val saverMaxRecordsOne = TestActorRef(Props(new PositionSaver(configWithOneMaxRecord)))

        saverMaxRecordsOne ! GetPositionRequest
        val initialPosition = expectMsgType[GetPositionResponse](5000 milliseconds)
        initialPosition.position should be(None)

        saverMaxRecordsOne ! EmitterResult("position")
        expectNoMessage(1000 milliseconds)

        val saverReloaded = TestActorRef(Props(new PositionSaver(configWithOneMaxRecord)))

        saverReloaded ! GetPositionRequest
        val savedPosition = expectMsgType[GetPositionResponse](5000 milliseconds)
        savedPosition.position should be(None)
      }
    }
  }
}
