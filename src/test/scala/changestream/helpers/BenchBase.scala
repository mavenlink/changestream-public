package changestream.helpers

import akka.actor.{ActorRefFactory, ActorSystem, Props}
import akka.testkit.{TestActorRef, TestProbe}
import com.github.mauricio.async.db.Configuration
import com.github.mauricio.async.db.mysql.MySQLConnection
import com.typesafe.config.ConfigFactory
import org.scalameter.api._
import org.scalameter.picklers.Implicits._

import scala.concurrent.duration._
import scala.language.postfixOps
import scala.concurrent.Await

class BenchBase extends Bench[Double] {
  lazy val executor = LocalExecutor(
    new Executor.Warmer.Default,
    Aggregator.min[Double],
    measurer)
  lazy val measurer = new Measurer.Default
  lazy val reporter = new LoggingReporter[Double]
  lazy val persistor = Persistor.None

  implicit val system = ActorSystem("changestream", ConfigFactory.load("test.conf"))
  implicit val ec = system.dispatcher
  val probe = TestProbe()
  val maker = (_: ActorRefFactory) => probe.ref

  val testConfig = ConfigFactory.load("test.conf")

  def getProbedActorOf[K](klass: Predef.Class[K], configPath: String = "changestream") =
    TestActorRef(Props(klass, maker, testConfig.getConfig(configPath)))

  protected val config = testConfig.getConfig("changestream.mysql")
  protected val mysqlConfig = new Configuration(
    config.getString("user"),
    config.getString("host"),
    config.getInt("port"),
    Some(config.getString("password"))
  )

  protected val connectionTimeout = config.getLong("timeout")
  protected val connection = new MySQLConnection(mysqlConfig)
  Await.result(connection.connect, connectionTimeout milliseconds)
  val result = connection.sendQuery("drop database if exists changestream_test")
    .flatMap(_ => connection.sendQuery("create database changestream_test"))
    .flatMap(_ => connection.sendQuery(s"""
                                          | CREATE TABLE changestream_test.users (
                                          |   `id` int(11) NOT NULL AUTO_INCREMENT,
                                          |   `username` varchar(32) DEFAULT NULL,
                                          |   `password` varchar(32) DEFAULT NULL,
                                          |   `login_count` int(11) NOT NULL DEFAULT '0',
                                          |   `bio` text DEFAULT NULL,
                                          |   PRIMARY KEY (`id`)
                                          | ) ENGINE=InnoDB
        """.stripMargin))
  Await.result(result, (connectionTimeout * 3) milliseconds)
}
