package changestream

import java.io.File

import akka.actor.{Props}
import akka.testkit.TestProbe
import changestream.actors.PositionSaver._
import changestream.actors.{PositionSaver, StdoutActor}
import changestream.events.{Delete, Insert, MutationWithInfo, Update}
import changestream.helpers.{Config, Database, Fixtures}
import com.github.mauricio.async.db.RowData
import com.typesafe.config.ConfigFactory

import scala.language.postfixOps
import akka.pattern.ask
import spray.json._

import scala.concurrent.duration._
import scala.concurrent.Await
import scala.io.Source
import scala.util.Random

class ChangeStreamISpec extends Database with Config {
  // Bootstrap the config
  val tempFile = File.createTempFile("positionSaverISpec", ".pos")
  System.setProperty("config.resource", "test.conf")
  System.setProperty("MYSQL_SERVER_ID", Random.nextLong.toString)
  System.setProperty("POSITION_SAVER_FILE_PATH", tempFile.getPath)
  System.setProperty("POSITION_SAVER_MAX_RECORDS", "2")
  System.setProperty("POSITION_SAVER_MAX_WAIT", "100000")
  ConfigFactory.invalidateCaches()

  // Wire in the probes
  lazy val positionSaver = ChangeStreamEventListener.system.actorOf(Props(new PositionSaver()), name = "positionSaverActor")
  lazy val emitter = ChangeStreamEventListener.system.actorOf(Props(new StdoutActor(_ => positionSaver)), name = "emitterActor")
  val emitterProbe = TestProbe()

  // Create the app thread
  val app = new Thread {
    override def run = ChangeStream.main(Array())
    override def interrupt = {
      Await.result(ChangeStreamEventListener.shutdown(), 60 seconds)
      super.interrupt
    }
  }

  override def beforeAll(): Unit = {
    ChangeStreamEventListener.setPositionSaver(positionSaver)
    ChangeStreamEventListener.setEmitter(emitterProbe.ref)
    app.start
    ensureConnected
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    tempFile.delete()
    super.afterAll()
  }

  def ensureConnected = {
    Thread.sleep(500)
    var c = 0
    while(!ChangeStream.isConnected && c < 50) {
      Thread.sleep(100)
      c += 1
    }
  }

  def ensureDisconnected = {
    Thread.sleep(500)
    var c = 0
    while(ChangeStream.isConnected && c < 50) {
      Thread.sleep(100)
      c += 1
    }
  }

  def expectMutation:MutationWithInfo = {
    val message = emitterProbe.expectMsgType[MutationWithInfo](5000.milliseconds)
    emitterProbe.forward(emitter)
    message
  }

  def getLiveBinLogPosition: String = {
    val result = Await.result(connection.sendQuery("show master status;"), 5000.milliseconds)
    result.rows match {
      case Some(resultSet) => {
        val row : RowData = resultSet.head
        val file = row("File").asInstanceOf[String]
        val position = row("Position").asInstanceOf[Long]
        s"${file}:${position}"
      }
      case None =>
        throw new Exception("Couldn't get binlog position")
    }
  }

  def getStoredBinLogPosition: String = {
    val bufferedSource = Source.fromFile(tempFile, "UTF-8")
    val position = bufferedSource.getLines.mkString
    bufferedSource.close
    position
  }

  def assertValidEvent( //scalastyle:ignore
                        mutation: String,
                        database: String = "changestream_test",
                        table: String = "users",
                        queryRowCount: Int = 1,
                        transactionCurrentRow: Int = 1,
                        primaryKeyField: String = "id",
                        currentRow: Int = 1,
                        sql: Option[String] = None,
                        isLastMutation: Boolean = false
                      ): Unit = {
    val message = expectMutation
    message.formattedMessage.isDefined should be(true)
    val json = message.formattedMessage.get.parseJson.asJsObject.fields

    json("mutation") should equal(JsString(mutation))
    json should contain key ("sequence")
    json("database") should equal(JsString(database))
    json("table") should equal(JsString(table))
    json("transaction").asJsObject.fields("current_row") should equal(JsNumber(transactionCurrentRow))
    if(isLastMutation) {
      json("transaction").asJsObject.fields("last_mutation") should equal(JsTrue)
    } else {
      json("transaction").asJsObject.fields.keys shouldNot contain("last_mutation")
    }
    json("query").asJsObject.fields("timestamp").asInstanceOf[JsNumber].value.toLong.compareTo(Fixtures.timestamp - 60000) should be(1)
    json("query").asJsObject.fields("row_count") should equal(JsNumber(queryRowCount))
    json("query").asJsObject.fields("current_row") should equal(JsNumber(currentRow))
    if(sql != None) {
      json("query").asJsObject.fields("sql") should equal(JsString(sql.get.trim))
    }
    json("primary_key").asJsObject.fields.keys should contain(primaryKeyField)
  }

  def validateNoEvents = emitterProbe.expectNoMessage(5 seconds)

  def waitAndClear(count: Int = 1) = {
    (1 to count).foreach(idx =>
      expectMutation
    )
  }

  "when handling an INSERT statement" should {
    "affecting a single row, generates a single insert event" in {
      queryAndWait(INSERT)

      assertValidEvent("insert", sql = Some(INSERT), isLastMutation = true)
    }
  }

  "when handling an INSERT statement" should {
    "affecting multiple rows, generates multiple insert events" in {
      queryAndWait(INSERT_MULTI)

      assertValidEvent("insert", queryRowCount = 2, currentRow = 1, transactionCurrentRow = 1, sql = Some(INSERT_MULTI))
      assertValidEvent("insert", queryRowCount = 2, currentRow = 2, transactionCurrentRow = 2, sql = Some(INSERT_MULTI), isLastMutation = true)
    }
  }

  "when handling an UPDATE statement" should {
    "affecting a single row, generates a single update event" in {
      queryAndWait(INSERT)
      waitAndClear()

      queryAndWait(UPDATE)
      assertValidEvent("update", sql = Some(UPDATE), isLastMutation = true)
    }
  }

  "when handling an UPDATE statement" should {
    "affecting multiple rows" in {
      queryAndWait(INSERT_MULTI)
      waitAndClear(2)

      queryAndWait(UPDATE_ALL)
      assertValidEvent("update", queryRowCount = 2, currentRow = 1, transactionCurrentRow = 1, sql = Some(UPDATE_ALL))
      assertValidEvent("update", queryRowCount = 2, currentRow = 2, transactionCurrentRow = 2, sql = Some(UPDATE_ALL), isLastMutation = true)
    }
  }

  "when handling an DELETE statement" should {
    "affecting a single row, generates a single delete event" in {
      queryAndWait(INSERT)
      waitAndClear()

      queryAndWait(DELETE)
      assertValidEvent("delete", sql = Some(DELETE), isLastMutation = true)
    }
    "affecting multiple rows" in {
      queryAndWait(INSERT)
      queryAndWait(INSERT)
      waitAndClear(2)

      queryAndWait(DELETE_ALL)
      assertValidEvent("delete", queryRowCount = 2, currentRow = 1, transactionCurrentRow = 1, sql = Some(DELETE_ALL))
      assertValidEvent("delete", queryRowCount = 2, currentRow = 2, transactionCurrentRow = 2, sql = Some(DELETE_ALL), isLastMutation = true)
    }
  }

  "when doing things in a transaction" should {
    "a successfully committed transaction" should {
      "buffers one change event to be able to properly tag the last event ina  transaction" in {
        queryAndWait("begin")
        queryAndWait(INSERT)
        queryAndWait(INSERT)
        queryAndWait(UPDATE_ALL)
        queryAndWait(DELETE_ALL)
        validateNoEvents

        queryAndWait("commit")
        assertValidEvent("insert", queryRowCount = 1, transactionCurrentRow = 1, currentRow = 1, sql = Some(INSERT))
        assertValidEvent("insert", queryRowCount = 1, transactionCurrentRow = 2, currentRow = 1, sql = Some(INSERT))
        assertValidEvent("update", queryRowCount = 2, transactionCurrentRow = 3, currentRow = 1, sql = Some(UPDATE_ALL))
        assertValidEvent("update", queryRowCount = 2, transactionCurrentRow = 4, currentRow = 2, sql = Some(UPDATE_ALL))
        assertValidEvent("delete", queryRowCount = 2, transactionCurrentRow = 5, currentRow = 1, sql = Some(DELETE_ALL))
        assertValidEvent("delete", queryRowCount = 2, transactionCurrentRow = 6, currentRow = 2, sql = Some(DELETE_ALL), isLastMutation = true)
      }
    }

    "a rolled back transaction" should {
      "generates no change events" in {
        queryAndWait("begin")
        queryAndWait(INSERT)
        queryAndWait(INSERT)
        queryAndWait(UPDATE_ALL)
        queryAndWait(DELETE_ALL)
        queryAndWait("rollback")
        validateNoEvents
      }
    }
  }

  "when starting up" should {
    "start from 'real time' when there is no last known position" in {
      ChangeStream.disconnectClient
      Await.result(positionSaver ? SavePositionRequest(None), 1000 milliseconds)

      queryAndWait(INSERT)

      ChangeStream.getConnectedAndWait(None)
      ensureConnected

      queryAndWait(UPDATE)

      expectMutation.mutation shouldBe a[Update]
    }

    "start reading from the last known position when there is a last known position and no override is passed" in {
      ChangeStream.disconnectClient
      Await.result(positionSaver ? SavePositionRequest(Some(getLiveBinLogPosition)), 1000 milliseconds)

      queryAndWait(INSERT)

      ChangeStream.getConnectedAndWait(None)
      ensureConnected

      queryAndWait(UPDATE)

      expectMutation.mutation shouldBe a[Insert]
      expectMutation.mutation shouldBe a[Update]
    }

    "start from override when override is passed" in {
      ChangeStream.disconnectClient
      Await.result(positionSaver ? SavePositionRequest(Some(getLiveBinLogPosition)), 1000 milliseconds)

      // this event should be skipped due to the override
      queryAndWait(INSERT)

      val overridePosition = getLiveBinLogPosition

      // this event should arrive first
      queryAndWait(UPDATE)

      ChangeStream.getConnectedAndWait(Some(overridePosition))
      ensureConnected

      // advance the live position to be "newer" than the override (should arrive second)
      queryAndWait(DELETE)

      expectMutation.mutation shouldBe a[Update]
      expectMutation.mutation shouldBe a[Delete]
    }

    "exit gracefully when a TERM signal is received" in {
      ChangeStream.disconnectClient
      Await.result(positionSaver ? SavePositionRequest(Some(getLiveBinLogPosition)), 1000 milliseconds)

      val startingPosition = getStoredBinLogPosition

      ChangeStream.getConnectedAndWait(None)
      ensureConnected

      queryAndWait(INSERT) // should not persist immediately because of the max events = 2
      val insertMutation = expectMutation
      insertMutation.mutation shouldBe a[Insert]
      Thread.sleep(1000)

      getStoredBinLogPosition should be(startingPosition)

      ChangeStream.disconnectClient
      ensureDisconnected
      Await.result(ChangeStreamEventListener.persistPosition, 60.seconds)

      getStoredBinLogPosition should be(insertMutation.nextPosition)

      queryAndWait(UPDATE) // should not immediately persist

      ChangeStream.getConnectedAndWait(None)
      ensureConnected

      queryAndWait(DELETE) // should persist because it is the second event processed by the saver
      queryAndWait(INSERT) // should not immediately persist

      expectMutation.mutation shouldBe a[Update]
      expectMutation.mutation shouldBe a[Delete]

      //at this point, our saved position is not fully up to date
      val finalMutation = expectMutation
      finalMutation.mutation shouldBe a[Insert]
      getStoredBinLogPosition shouldNot be(finalMutation.nextPosition)

      app.interrupt

      //should save any pending position pre-exit
      getStoredBinLogPosition should be(finalMutation.nextPosition)
    }
  }
}

