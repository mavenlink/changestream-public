package changestream.actors

import java.io._
import java.nio.charset.StandardCharsets

import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{Failure, Success}
import akka.actor.{Actor, ActorRef, ActorRefFactory, Cancellable}
import changestream.actors.PositionSaver.EmitterResult
import changestream.events.MutationWithInfo
import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.model.{ObjectMetadata, PutObjectRequest, PutObjectResult}
import com.github.dwhjames.awswrap.s3.AmazonS3ScalaClient
import com.typesafe.config.{Config, ConfigFactory}
import org.joda.time.DateTime
import org.slf4j.LoggerFactory
import kamon.Kamon
import kamon.metric.MeasurementUnit

import scala.concurrent.{Await, Future}

object S3Actor {
  case object FlushRequest
}

class S3Actor(getNextHop: ActorRefFactory => ActorRef,
              config: Config = ConfigFactory.load().getConfig("changestream")) extends Actor {
  import S3Actor.FlushRequest

  protected val nextHop = getNextHop(context)
  protected val log = LoggerFactory.getLogger(getClass)
  protected val successMetric = Kamon.counter("changestream.emitter.total").withTag("emitter", "s3").withTag("result","success")
  protected val failureMetric = Kamon.counter("changestream.emitter.total").withTag("emitter", "s3").withTag("result","failure")
  protected val fileSizeSuccessMetric = Kamon.histogram("changestream.emitter.s3_bytes", MeasurementUnit.information.bytes).withTag("result", "success")
  protected val fileSizeFailureMetric = Kamon.histogram("changestream.emitter.s3_bytes", MeasurementUnit.information.bytes).withTag("result", "failure")
  protected val inFlightMetric = Kamon.rangeSampler("changestream.emitter.in_flight").withTag("emitter", "s3")

  protected implicit val ec = context.dispatcher

  protected val BUFFER_TEMP_DIR = config.getString("aws.s3.buffer-temp-dir")
  protected lazy val bufferDirectory = new File(BUFFER_TEMP_DIR)
  protected val BUCKET = config.getString("aws.s3.bucket")
  protected val KEY_PREFIX = config.getString("aws.s3.key-prefix") match {
    case s if s.endsWith("/") => s
    case s => s"$s/"
  }
  protected val BATCH_SIZE = config.getLong("aws.s3.batch-size")
  protected val MAX_WAIT = config.getLong("aws.s3.flush-timeout").milliseconds
  protected val TIMEOUT = config.getInt("aws.timeout")

  protected var cancellableSchedule: Option[Cancellable] = None
  protected def setDelayedFlush = {
    val scheduler = context.system.scheduler
    cancellableSchedule = Some(scheduler.scheduleOnce(MAX_WAIT) { self ! FlushRequest })
  }
  protected def cancelDelayedFlush = cancellableSchedule.foreach(_.cancel())

  protected lazy val client = new AmazonS3ScalaClient(
    new DefaultAWSCredentialsProviderChain(),
    new ClientConfiguration().
      withConnectionTimeout(TIMEOUT),
    Regions.fromName(config.getString("aws.region"))
  )

  // Mutable State!!
  protected var lastPosition = ""
  protected var currentBatchSize = 0
  protected var bufferFile: File = getNextFile
  protected var bufferWriter: BufferedWriter = getWriterForFile
  // End Mutable State!!

  // Wrap the Java IO
  protected def getNextFile = BUFFER_TEMP_DIR match {
    case "" =>
      File.createTempFile("buffer-", ".json")
    case _ if bufferDirectory.exists && bufferDirectory.canWrite =>
      File.createTempFile("buffer-", ".json", bufferDirectory)
    case _ =>
      log.error("Failed to write to buffer directory {}, make sure it exists and is writeable. Using the system default temp dir instead.", bufferDirectory)
      File.createTempFile("buffer-", ".json")
  }
  protected def getWriterForFile = {
    val streamWriter = new OutputStreamWriter(new FileOutputStream(bufferFile), StandardCharsets.UTF_8)
    new BufferedWriter(streamWriter)
  }

  protected def bufferMessage(message: String) = {
    bufferWriter.write(message)
    bufferWriter.newLine()
    currentBatchSize += 1
  }

  protected def getMetadata(length: Long, sseAlgorithm: String = "AES256") = {
    val metadata = new ObjectMetadata()
    metadata.setSSEAlgorithm(sseAlgorithm)
    metadata.setContentLength(length)
    metadata
  }

  protected def getMessageBatch = {
    val batchFile = bufferFile

    bufferWriter.close()
    currentBatchSize = 0
    bufferFile = getNextFile
    bufferWriter = getWriterForFile

    batchFile
  }

  protected def putFile(file: File, key: String = ""): Future[PutObjectResult] = {
    val objectKey = key match {
      case "" => file.getName
      case _ => key
    }
    val metadata = getMetadata(file.length)
    client.putObject(new PutObjectRequest(BUCKET, s"${KEY_PREFIX}${objectKey}", file).withMetadata(metadata))
  }

  override def preStart() = {
    val file = new File(s"${BUFFER_TEMP_DIR}test.txt")
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write("test")
    bw.close()

    val testPutFuture = putFile(file)
    testPutFuture onComplete {
      case Success(_: PutObjectResult) =>
        file.delete()
      case Failure(exception) =>
        log.error(s"Failed to create test object in S3 bucket ${BUCKET} at key ${KEY_PREFIX}test.txt.", exception)
        throw exception
    }

    Await.result(testPutFuture, TIMEOUT milliseconds)
    log.info(s"Ready to push messages to bucket ${BUCKET} with key prefix ${KEY_PREFIX}")
  }

  override def postStop() = {
    cancelDelayedFlush
    bufferWriter.close()
    bufferFile.delete()
    client.shutdown()
  }

  def receive = {
    case MutationWithInfo(_, pos, _, _, Some(message: String)) =>
      log.debug("Received message of size {}", message.length)
      log.trace("Received message: {}", message)

      inFlightMetric.increment()

      cancelDelayedFlush

      lastPosition = pos
      bufferMessage(message)
      currentBatchSize match {
        case BATCH_SIZE => flush
        case _ => setDelayedFlush
      }

    case FlushRequest =>
      flush
  }

  protected def flush = {
    log.debug("Flushing {} messages to S3.", currentBatchSize)

    val position = lastPosition

    val batchSize = currentBatchSize
    val now = DateTime.now
    val datePrefix = f"${now.getYear}/${now.getMonthOfYear}%02d/${now.getDayOfMonth}%02d/${now.getHourOfDay}%02d"
    val key = s"${datePrefix}/${System.nanoTime}-${batchSize}.json"
    val file = getMessageBatch
    val request = putFile(file, key)

    val s3Url = s"${BUCKET}/${KEY_PREFIX}${key}"

    request onComplete {
      case Success(_: PutObjectResult) =>
        log.info(s"Successfully saved ${batchSize} messages (${file.length} bytes) to ${s3Url}.")

        successMetric.increment(batchSize)
        fileSizeSuccessMetric.record(file.length)
        inFlightMetric.decrement(batchSize)

        file.delete()
        nextHop ! EmitterResult(position, batchSize, Some(s3Url))
      case Failure(exception) =>
        log.error(s"Failed to save ${batchSize} messages from ${file.getName} (${file.length} bytes) to ${s3Url}.", exception)

        failureMetric.increment(batchSize)
        fileSizeFailureMetric.record(file.length)
        inFlightMetric.decrement(batchSize)

        throw exception
    }
  }
}
