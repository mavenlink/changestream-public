package changestream.actors

import java.util.UUID

import akka.actor.{ Actor, ActorRef, ActorRefFactory }
import changestream.events.MutationWithInfo

import changestream.events._
import kamon.Kamon
import org.slf4j.LoggerFactory

class TransactionActor(getNextHop: ActorRefFactory => ActorRef) extends Actor {
  protected val log = LoggerFactory.getLogger(getClass)
  protected val batchSizeMetric = Kamon.histogram("changestream.binlog_event.row_count").withoutTags()
  protected val transactionSizeMetric = Kamon.histogram("changestream.transaction.row_count").withoutTags()

  protected val nextHop = getNextHop(context)

  /** Mutable State! */
  protected var mutationCount: Long = 1
  protected var currentGtid: Option[String] = None
  protected var previousMutation: Option[MutationWithInfo] = None

  def receive = {
    case BeginTransaction =>
      log.debug("Received BeginTransacton")
      mutationCount = 1
      currentGtid = Some(UUID.randomUUID.toString)
      previousMutation = None

    case Gtid(guid) =>
      log.debug("Received GTID for transaction: {}", guid)
      currentGtid = Some(guid)

    case event: MutationWithInfo =>
      log.debug("Received Mutation for tableId: {}", event.mutation.tableId)
      batchSizeMetric.record(event.mutation.rows.length)

      currentGtid match {
        case None =>
          nextHop ! event
        case Some(gtid) =>
          previousMutation.foreach { mutation =>
            log.debug("Adding transaction info and forwarding to the {} actor.", nextHop.path.name)
            nextHop ! mutation
          }
          previousMutation = Some(event.copy(
            transaction = Some(TransactionInfo(
              gtid = gtid,
              currentRow = mutationCount
            ))
          ))
          mutationCount += event.mutation.rows.length
      }

    case CommitTransaction(position) =>
      log.debug("Received Commit with position {}", position)
      previousMutation.foreach { mutation =>
        log.debug("Adding transaction info and forwarding to the {} actor.", nextHop.path.name)
        nextHop ! mutation.copy(
          transaction = mutation.transaction.map { txInfo =>
            txInfo.copy(lastMutationInTransaction = true)
          },
          // TODO: this is unfortunate... because we are now essentially saving the "last safe position" we are guaranteed to replay events when we shut down un-gracefully
          nextPosition = mutation.nextPosition.split(":")(0) + ":" + position.toString
        )
      }
      transactionSizeMetric.record(mutationCount)
      mutationCount = 1
      currentGtid = None
      previousMutation = None

    case RollbackTransaction =>
      log.debug("Received Rollback")

      // TODO: this probably doesn't work for mysql configurations that send a rollback event (vs only sending committed events).. consider removing the rollback handling
      previousMutation.foreach { mutation =>
        log.debug("Adding transaction info and forwarding to the {} actor.", nextHop.path.name)
        nextHop ! mutation.copy(
          transaction = mutation.transaction.map { txInfo =>
            txInfo.copy(lastMutationInTransaction = true)
          }
        )
      }
      transactionSizeMetric.record(mutationCount)
      mutationCount = 1
      currentGtid = None
      previousMutation = None
  }
}
