package changestream

import changestream.helpers.{Base, Config}
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream
import com.typesafe.config.ConfigFactory

import scala.language.postfixOps

class RowsQueryDeserializerSpec extends Base with Config {
  val sqlString = "select 'foo';"
  val bytes = Array[Byte](1) ++ sqlString.getBytes
  var inputStream = new ByteArrayInputStream(bytes)

  before {
    inputStream = new ByteArrayInputStream(bytes)
    inputStream.enterBlock(bytes.length)
  }

  "When not configured to truncate the SQL" should {
    "Capture all of the SQL" in {
      RowsQueryDeserializer.deserialize(inputStream)

      ChangeStreamEventDeserializer.lastQuery should be(Some(sqlString))
      inputStream.available() should be (0)
    }
  }

  "When configured to truncate the SQL at a given length" should {
    "Capture all of the SQL when the SQL is shorter or equal to the limit" in {
      val bigLimitConfig = ConfigFactory.parseString("changestream.sql-character-limit = 1000").withFallback(testConfig).getConfig("changestream")
      ChangestreamEventDeserializerConfig.setConfig(bigLimitConfig)
      RowsQueryDeserializer.deserialize(inputStream)

      ChangeStreamEventDeserializer.lastQuery should be(Some(sqlString))
      inputStream.available() should be (0)
    }

    "When the SQL is longer than the limit" should {
      "Only capture <limit> characters of SQL" in {
        val smallLimitConfig = ConfigFactory.parseString("changestream.sql-character-limit = 5").withFallback(testConfig).getConfig("changestream")
        ChangestreamEventDeserializerConfig.setConfig(smallLimitConfig)
        RowsQueryDeserializer.deserialize(inputStream)

        ChangeStreamEventDeserializer.lastQuery should be(Some(sqlString.substring(0, 5)))
        inputStream.available() should be (0)
      }
    }
  }
}
