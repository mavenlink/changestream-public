package changestream.actors

import java.text.{DateFormat, SimpleDateFormat}
import java.util
import java.util.TimeZone
import java.nio.charset.StandardCharsets;

import akka.actor.{Actor, ActorRef, ActorRefFactory, Props}
import akka.pattern.{ask, pipe}
import changestream.events._
import com.typesafe.config.{Config, ConfigFactory}
import spray.json._
import DefaultJsonProtocol._
import akka.util.Timeout
import changestream.actors.EncryptorActor.Plaintext
import changestream.{IntegrityVerifier}
import kamon.Kamon
import kamon.metric.MeasurementUnit
import org.slf4j.LoggerFactory

import scala.concurrent.duration._
import scala.language.postfixOps
import scala.collection.immutable.ListMap

object JsonFormatterActor {
  val dateFormatter: DateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")
  val textFieldSizeLimit: Integer = 16000 // 16KB
  val truncationText: String = "...[TRUNCATED]"
  dateFormatter.setTimeZone(TimeZone.getTimeZone("UTC"))

  /**
    * Current columnType to java type mapping is following:
    * <pre>
    * {@link ColumnType#TINY}: Integer
    * {@link ColumnType#SHORT}: Integer
    * {@link ColumnType#LONG}: Integer
    * {@link ColumnType#INT24}: Integer
    * {@link ColumnType#YEAR}: Integer
    * {@link ColumnType#ENUM}: Integer
    * {@link ColumnType#SET}: Long
    * {@link ColumnType#LONGLONG}: Long
    * {@link ColumnType#FLOAT}: Float
    * {@link ColumnType#DOUBLE}: Double
    * {@link ColumnType#BIT}: java.util.BitSet
    * {@link ColumnType#DATETIME}: java.util.Date
    * {@link ColumnType#DATETIME_V2}: java.util.Date
    * {@link ColumnType#NEWDECIMAL}: java.math.BigDecimal
    * {@link ColumnType#TIMESTAMP}: java.sql.Timestamp
    * {@link ColumnType#TIMESTAMP_V2}: java.sql.Timestamp
    * {@link ColumnType#DATE}: java.sql.Date
    * {@link ColumnType#TIME}: java.sql.Time
    * {@link ColumnType#TIME_V2}: java.sql.Time
    * {@link ColumnType#VARCHAR}: String
    * {@link ColumnType#VAR_STRING}: String
    * {@link ColumnType#STRING}: String
    * {@link ColumnType#BLOB}: byte[]
    * {@link ColumnType#GEOMETRY}: byte[]
    * </pre>
    *
    */
  def getJsValueOrNone(javaVal: java.io.Serializable):Any = javaVal match {
    case v: util.BitSet => v.size match {
      case 1 => // return single bit as true/false value
        v.get(0).toJson
      case _ =>
        (0 to v.size - 1).map(v.get(_)).toJson //return many bits as an array of true/false
    }
    case timestamp: java.sql.Timestamp =>
      JsNumber(timestamp.getTime)
    case time: java.sql.Time =>
      JsString(time.toString)
    case date: java.sql.Date =>
      JsString(date.toString)
    case d: java.util.Date =>
      JsString(dateFormatter.format(d))
    case s: String =>
      JsString(s)
    case v: java.lang.Float =>
      JsNumber(v.toFloat)
    case v: java.lang.Double =>
      JsNumber(v)
    case v: java.math.BigDecimal =>
      JsNumber(v)
    case v: java.lang.Number =>
      JsNumber(v.longValue)
    case null => //scalastyle:ignore
      JsNull
    case byte: Array[Byte] =>  // This adds support for TEXT and blobs
      var text = new String(byte, StandardCharsets.UTF_8)
      text = text.slice(0, textFieldSizeLimit) // Does the truncation regardless of size to simplify logic

      if(byte.length > textFieldSizeLimit) { // Check if text field is longer than 16KB, which is the arbitrary value we have chosen to support
        // This is done to reduce the size of the data that is being sent over the web
        text += truncationText
      }

      JsString(text)
    case _ => // unknown/unsupported data type
      None
  }
}

class JsonFormatterActor (
                           getNextHop: ActorRefFactory => ActorRef,
                           config: Config = ConfigFactory.load().getConfig("changestream")
                         ) extends Actor {
  import JsonFormatterActor._

  protected val nextHop = getNextHop(context)
  protected val log = LoggerFactory.getLogger(getClass)
  protected val jsonSizeMetric = Kamon.histogram("changestream.json.bytes", MeasurementUnit.information.bytes).withoutTags()
  protected val jsonTimingMetric = Kamon.timer("changestream.json.time")

  protected val includeData = config.getBoolean("include-data")
  protected val prettyPrint = config.getBoolean("pretty-print")
  protected val encryptData = if(!includeData) false else config.getBoolean("encryptor.enabled")

  protected lazy val encryptorActor = context.actorOf(Props(new EncryptorActor(config.getConfig("encryptor"))), name = "encryptorActor")
  protected implicit val TIMEOUT = Timeout(config.getLong("encryptor.timeout") milliseconds)
  protected implicit val ec = context.dispatcher

  def receive = {
    case message: MutationWithInfo if message.columns.isDefined => {

      log.debug("Received {} for table {}.{}", message.mutation, message.mutation.database, message.mutation.tableName)

      val primaryKeys = message.columns.get.columns.collect({ case col if col.isPrimary => col.name })
      val rowData = getRowData(message)
      val oldRowData = getOldRowData(message)

      rowData.indices.foreach({ idx =>
        val timer = jsonTimingMetric
          .withTag("database", message.mutation.database)
          .withTag("table", message.mutation.tableName)
          .start()

        val row = rowData(idx)
        val oldRow = oldRowData.map(_(idx))
        val pkInfo = ListMap(primaryKeys.map({
          case k:String => k -> row.getOrElse(k, JsNull)
        }):_*)

        val payload =
          getJsonHeader(message, pkInfo, row, idx, rowData.length) ++
          transactionInfo(message, idx, rowData.length) ++
          getJsonRowData(row) ++
          updateInfo(oldRow)
        val json = JsObject(payload)

        if(encryptData) {
          log.debug("Encrypting JSON event and sending to the {} actor", nextHop.path.name)
          val encryptRequest = Plaintext(json)
          ask(encryptorActor, encryptRequest).map {
            case v: JsValue =>
              val payload = prepMessagePayload(message, v)
              timer.stop()
              payload
          } pipeTo nextHop onFailure {
            case e: Exception =>
              log.error("Failed to encrypt JSON event.", e)
              throw e
          }
        }
        else {
          log.debug("Sending JSON event to the {} actor.", nextHop.path.name)
          IntegrityVerifier.matchesTableAndSql(message.mutation.tableName, message.mutation.sql.getOrElse(""), message.nextPosition, "JsonFormatterActor")
          nextHop ! prepMessagePayload(message, json)
          timer.stop()
        }
      })
    }
  }

  protected def prepMessagePayload(message: MutationWithInfo, json: JsValue) = {
    val jsonString = getJsonString(json)

    jsonSizeMetric
      .withTag("database", message.mutation.database)
      .withTag("table", message.mutation.tableName)
      .record(jsonString.length)

    message.copy(formattedMessage = Some(jsonString))
  }

  protected def getJsonString(v: JsValue) = prettyPrint match {
    case true => v.prettyPrint
    case false => v.compactPrint
  }

  protected def getRowData(message: MutationWithInfo) = {
    val columns = message.columns.get.columns
    val mutation = message.mutation

    mutation.rows.map(row =>
      ListMap(columns.indices.map({
        case idx if mutation.includedColumns.get(idx) =>
          columns(idx).name -> getJsValueOrNone(row(idx))
        case _ => // this would happen for example when the schema changes to add additional columns
          ListMap.empty
      }).collect({
        case (k:String, v:JsValue) => k -> v
      }):_*)
    )
  }

  protected def getOldRowData(message: MutationWithInfo) = {
    val columns = message.columns.get.columns
    val mutation = message.mutation

    mutation match {
      case update:Update =>
        Some(update.oldRows.map(row =>
          ListMap(columns.indices.map({
            case idx if mutation.includedColumns.get(idx) =>
              columns(idx).name -> getJsValueOrNone(row(idx))
            case _ => // this would happen for example when the schema changes to add additional columns
              ListMap.empty
          }).collect({
            case (k:String, v:JsValue) => k -> v
          }):_*)
        ))
      case _ => None
    }
  }

  protected def transactionInfo(message: MutationWithInfo, rowOffset: Long, rowsTotal: Long): ListMap[String, JsValue] = {
    message.transaction match {
      case Some(txn) => ListMap(
        "transaction" -> JsObject(ListMap(
          "id" -> txn.gtid.toJson,
          "current_row" -> (txn.currentRow + rowOffset).toJson
        ) ++ (((rowOffset == rowsTotal - 1) && txn.lastMutationInTransaction) match {
          case true => ListMap(
            "last_mutation" -> JsTrue
          )
          case false => ListMap.empty
        })
      ))
      case None => ListMap.empty
    }
  }

  protected def getJsonHeader(
                              message: MutationWithInfo,
                              pkInfo: ListMap[String, JsValue],
                              rowData: ListMap[String, JsValue],
                              currentRow: Long,
                              rowsTotal: Long
                             ): ListMap[String, JsValue] = {
    ListMap(
      "mutation" -> JsString(message.mutation.toString),
      "sequence" -> JsNumber(message.mutation.sequence + currentRow),
      "database" -> JsString(message.mutation.database),
      "table" -> JsString(message.mutation.tableName),
      "query" -> JsObject(
        "timestamp" -> JsNumber(message.mutation.timestamp),
        "sql" -> JsString(message.mutation.sql.getOrElse("")),
        "row_count" -> JsNumber(rowsTotal),
        "current_row" -> JsNumber(currentRow + 1)
      ),
      "primary_key" -> JsObject(pkInfo)
    )
  }

  protected def getJsonRowData(rowData: ListMap[String, JsValue]): ListMap[String, JsValue] = includeData match {
    case true => ListMap("row_data" -> JsObject(rowData))
    case false => ListMap.empty
  }

  protected def updateInfo(oldRowData: Option[ListMap[String, JsValue]]): ListMap[String, JsValue] = includeData match {
    case true => oldRowData.map({ row => ListMap("old_row_data" -> JsObject(row)) }).getOrElse(ListMap.empty)
    case false => ListMap.empty
  }
}
