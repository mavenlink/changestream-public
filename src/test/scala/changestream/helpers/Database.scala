package changestream.helpers

import com.github.mauricio.async.db.Configuration
import com.github.mauricio.async.db.mysql.MySQLConnection
import org.scalatest.BeforeAndAfterAll

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps

trait Database extends Base with Config with BeforeAndAfterAll {
  val INSERT = s"""INSERT INTO changestream_test.users VALUES (
                  | NULL, "peter", "hello", 10, "I Am Peter", 0,
                  | 0.18289882, 0.23351681013224323546495497794239781796932220458984375, NOW(), CURDATE(), CURTIME(), NOW(), null, null, null)
        """.stripMargin.trim
  val INSERT_MULTI = s"""${INSERT},
       | (NULL, "multiguy", "stupid", 1000000, "Leeloo Dallas Multiguy", 0, 0.18289882,
       | 0.23351681013224323546495497794239781796932220458984375, NOW(), CURDATE(), CURTIME(), NOW(), null, null, null)""".stripMargin
  val UPDATE = s"""UPDATE changestream_test.users set username = "username2", password = "password2", login_count = login_count + 1, bio = "bio2""""
  val UPDATE_ALL = "UPDATE changestream_test.users set login_count = login_count + 1"
  val DELETE = "DELETE from changestream_test.users LIMIT 1"
  val DELETE_ALL = "DELETE from changestream_test.users"

  protected val config = testConfig.getConfig("changestream.mysql")
  protected val mysqlConfig = new Configuration(
    config.getString("user"),
    config.getString("host"),
    config.getInt("port"),
    Some(config.getString("password"))
  )
  protected val connectionTimeout = config.getLong("timeout")
  protected val connection = new MySQLConnection(mysqlConfig)

  def queryAndWait(sql: String): Unit = Await.result(connection.sendQuery(sql), connectionTimeout milliseconds)

  override def beforeAll(): Unit = {
    try {
      Await.result(connection.connect, connectionTimeout milliseconds)
    } catch {
      case e: Exception =>
        println(s"Could not connect to MySQL server for Metadata: ${e.getMessage}\n${e.getStackTrace.mkString("\n")}")
    }
  }

  override def afterAll(): Unit = {
    Await.result(connection.disconnect, connectionTimeout milliseconds)
  }

  before {
    try {
      val result = connection.sendQuery("drop database if exists changestream_test")
        .flatMap(_ => connection.sendQuery("create database changestream_test"))
        .flatMap(_ => connection.sendQuery(s"""
                                              | CREATE TABLE changestream_test.users (
                                              |   `id` int(11) NOT NULL AUTO_INCREMENT,
                                              |   `username` varchar(32) DEFAULT NULL,
                                              |   `password` varchar(32) DEFAULT NULL,
                                              |   `login_count` int(11) NOT NULL DEFAULT '0',
                                              |   `bio` text DEFAULT NULL,
                                              |   `two_bit_field` bit DEFAULT NULL,
                                              |   `float_field` float DEFAULT NULL,
                                              |   `big_decimal_field` decimal DEFAULT NULL,
                                              |   `java_util_date` datetime DEFAULT NULL,
                                              |   `java_sql_date` date DEFAULT NULL,
                                              |   `java_sql_time` time DEFAULT NULL,
                                              |   `java_sql_timestamp` timestamp DEFAULT NOW(),
                                              |   `non_truncated_text` blob DEFAULT NULL,
                                              |   `text_that_should_be_truncated` blob DEFAULT NULL,
                                              |   `random_bytes_blob` blob DEFAULT NULL,
                                              |   PRIMARY KEY (`id`)
                                              | ) ENGINE=InnoDB;
          """.stripMargin))
      Await.result(result, (connectionTimeout * 3) milliseconds)
    } catch {
      case e: Exception =>
        println(s"Could not initialize database for tests ${e.getMessage}\n${e.getStackTrace.mkString("\n")}")
    }
  }

  after {
    try {
      val result = connection.sendQuery("drop database if exists changestream_test")
      Await.result(result, connectionTimeout milliseconds)
    } catch {
      case e: Exception =>
        println(s"Could not tear down database for tests ${e.getMessage}\n${e.getStackTrace.mkString("\n")}")
    }
  }
}
