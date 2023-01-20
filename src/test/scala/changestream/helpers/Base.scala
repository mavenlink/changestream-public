package changestream.helpers

import akka.actor.ActorSystem
import akka.testkit.{DefaultTimeout, ImplicitSender, TestKit}
import com.typesafe.config.ConfigFactory
import org.scalatest._

class Base extends TestKit(ActorSystem("changestream_test", ConfigFactory.load("test.conf")))
    with DefaultTimeout
    with ImplicitSender
    with Matchers
    with Inside
    with WordSpecLike
    with BeforeAndAfterAll
    with BeforeAndAfter {
  implicit val ec = system.dispatcher

  override def afterAll() = {
    TestKit.shutdownActorSystem(system)
  }
}
