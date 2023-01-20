package changestream.actors

import akka.actor.{ActorRefFactory, Props}
import akka.testkit.{TestActorRef, TestProbe}
import changestream.events.MutationWithInfo
import changestream.events._
import changestream.helpers.{Base, Fixtures}

class TransactionActorSpec extends Base {
  val probe = TestProbe()
  val maker = (_: ActorRefFactory) => probe.ref
  val transactionActor = TestActorRef(Props(classOf[TransactionActor], maker))

  val GUID_LENGTH = 36
  val (mutationNoTransaction, _, _) = Fixtures.mutationWithInfo("insert", rowCount = 1, transactionInfo = false, columns = false)
  val (mutationFirstOutput, _, _) = Fixtures.mutationWithInfo("insert", rowCount = 1, transactionCurrentRow = 1, transactionInfo = true, isLastChangeInTransaction = false, columns = false)
  val (mutationNextOutput, _, _) = Fixtures.mutationWithInfo("insert", rowCount = 1, transactionCurrentRow = 2, transactionInfo = true, isLastChangeInTransaction = false, columns = false)
  val (mutationLastOutput, _, _) = Fixtures.mutationWithInfo("insert", rowCount = 1, transactionCurrentRow = 3, transactionInfo = true, isLastChangeInTransaction = true, columns = false)
  val (mutationMultiOutput, _, _) = Fixtures.mutationWithInfo("insert", rowCount = 10, transactionCurrentRow = 1, transactionInfo = true, isLastChangeInTransaction = false, columns = false)
  val (mutationMultiNextOutput, _, _) = Fixtures.mutationWithInfo("insert", rowCount = 1, transactionCurrentRow = 11, transactionInfo = true, isLastChangeInTransaction = false, columns = false)
  val mutationFirstInput = mutationFirstOutput.copy(transaction = None)
  val mutationNextInput = mutationNextOutput.copy(transaction = None)
  val mutationLastInput = mutationLastOutput.copy(transaction = None)
  val mutationMultiInput = mutationMultiOutput.copy(transaction = None)
  val mutationMultiNextInput = mutationMultiNextOutput.copy(transaction = None)
  val gtid = "9fc4cdc0-8f3b-11e6-a5b1-e39f73659fee:24"

  def expectMessageFuzzyGuidMatch(mutation: MutationWithInfo) = {
    val msg = probe.expectMsgType[MutationWithInfo]
    val expectObj = mutation.copy(
      nextPosition = msg.nextPosition,
      transaction = mutation.transaction.map(info => info.copy(gtid = msg.transaction.get.gtid))
    )
    msg should be(expectObj)
  }

  "When receiving a TransactionEvent" should {
    "when not in a transaction, we should get an event right away" in {
      transactionActor ! mutationNoTransaction

      probe.expectMsg[MutationWithInfo](mutationNoTransaction)
    }

    "when we are in a transaction" should {
      "buffer the first change that arrives" in {
        transactionActor ! BeginTransaction

        transactionActor ! mutationFirstInput
        probe.expectNoMessage
      }

      "when receiving a subsequent event should emit previous event" in {
        transactionActor ! BeginTransaction

        transactionActor ! mutationFirstInput

        transactionActor ! mutationNextInput
        expectMessageFuzzyGuidMatch(mutationFirstOutput)

        transactionActor ! mutationLastInput
        expectMessageFuzzyGuidMatch(mutationNextOutput)
      }

      "when receiving a subsequent event should emit correct currentRow when previous event contains multiple rows" in {
        transactionActor ! BeginTransaction

        transactionActor ! mutationMultiInput

        transactionActor ! mutationMultiNextInput
        expectMessageFuzzyGuidMatch(mutationMultiOutput)

        transactionActor ! mutationLastInput
        expectMessageFuzzyGuidMatch(mutationMultiNextOutput)
      }

      "when committing a transaction should emit last event with correct flag" in {
        transactionActor ! BeginTransaction

        transactionActor ! mutationFirstInput
        transactionActor ! mutationNextInput
        transactionActor ! mutationLastInput
        probe.receiveN(2)

        transactionActor ! CommitTransaction(1)
        expectMessageFuzzyGuidMatch(mutationLastOutput)
      }
    }

    "not be in a transaction state after a rollback" in {
      transactionActor ! BeginTransaction
      transactionActor ! mutationFirstInput
      transactionActor ! RollbackTransaction
      /**
        * Modifications to nontransactional tables cannot be rolled back.
        * If a transaction that is rolled back includes modifications to
        * nontransactional tables, the entire transaction is logged with
        * a ROLLBACK statement at the end to ensure that the modifications
        * to those tables are replicated.
        *
        * http://dev.mysql.com/doc/refman/5.7/en/binary-log.html
        */
      probe.receiveN(1)

      transactionActor ! mutationNextInput
      expectMessageFuzzyGuidMatch(mutationNextInput)
    }

    "not be in a transaction state after a commit" in {
      transactionActor ! BeginTransaction
      transactionActor ! mutationFirstInput
      transactionActor ! CommitTransaction(1)
      probe.receiveN(1)

      transactionActor ! mutationNextInput
      expectMessageFuzzyGuidMatch(mutationNextInput)
    }

    "expect the same transaction id for mutations in the same transaction" in {
      transactionActor ! BeginTransaction

      transactionActor ! mutationFirstInput
      transactionActor ! mutationNextInput

      transactionActor ! CommitTransaction(1)

      val m1 = probe.expectMsgType[MutationWithInfo]
      val m2 = probe.expectMsgType[MutationWithInfo]
      inside(m1.transaction) { case Some(info1) =>
        inside(m2.transaction) { case Some(info2) =>
          info1.gtid should be(info2.gtid)
          info1.currentRow should be(1)
          info2.currentRow should be(2)
          info1.lastMutationInTransaction should be(false)
          info2.lastMutationInTransaction should be(true)
        }
      }
    }

    "When receiving a GtidEvent event" should {
      "Expect the TransactionId to be overwritten with the gtid from MySQL" in {
        transactionActor ! BeginTransaction
        transactionActor ! Gtid(gtid)

        transactionActor ! mutationFirstInput
        transactionActor ! CommitTransaction(1)

        inside(probe.expectMsgType[MutationWithInfo].transaction) {
          case Some(info) =>
            info.gtid should be(gtid)
        }
      }
    }
  }
}
