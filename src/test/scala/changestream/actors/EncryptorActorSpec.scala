package changestream.actors

import akka.actor.Props
import akka.testkit.EventFilter
import akka.testkit.TestActorRef
import changestream.actors.EncryptorActor._
import changestream.helpers.{Base, Config}
import com.typesafe.config.ConfigFactory
import spray.json._

class EncryptorActorSpec extends Base with Config {
  val config = ConfigFactory.
    parseString("changestream.encryptor.encrypt-fields = \"do_encrypt, do_encrypt_hash,parent.do_encrypt_hash\"").
    withFallback(testConfig).
    getConfig("changestream.encryptor")
  val encryptorActor = TestActorRef(Props(classOf[EncryptorActor], config))

  val sourceObject = JsObject(
    "no_encrypt" -> JsString("a"),
    "no_encrypt_hash" -> JsObject("aa" -> JsNumber(1), "bb" -> JsNumber(2)),
    "do_encrypt" -> JsString("b"),
    "do_encrypt_hash" -> JsObject("aa" -> JsNumber(1), "bb" -> JsNumber(2)),
    "parent" -> JsObject(
      "do_encrypt_hash" -> JsObject("aa" -> JsNumber(1), "bb" -> JsNumber(2)),
      "no_encrypt_hash" -> JsObject("cc" -> JsNumber(3), "dd" -> JsNumber(4))
    )
  )

  "When encrypting/decrypting data" should {
    "expect encrypt -> decrypt to result in same plaintext for a single message" in {
      encryptorActor ! Plaintext(sourceObject)
      val encryptResponse = expectMsgType[JsObject]

      encryptorActor ! Ciphertext(encryptResponse)
      val decryptResponse = expectMsgType[JsObject]

      sourceObject.compactPrint should be(decryptResponse.compactPrint)
    }

    "expect encrypt to correctly encrypt all fields in the message marked for encrypt" in {
      encryptorActor ! Plaintext(sourceObject)
      val encryptResponse = expectMsgType[JsObject]

      encryptResponse.fields("no_encrypt") should be(JsString("a"))
      encryptResponse.fields("no_encrypt_hash") should be(JsObject("aa" -> JsNumber(1), "bb" -> JsNumber(2)))
      encryptResponse.fields("do_encrypt").isInstanceOf[JsString] should be(true)
      encryptResponse.fields("do_encrypt_hash").isInstanceOf[JsString] should be(true)
      encryptResponse.fields("parent").asJsObject.fields("no_encrypt_hash") should be(JsObject("cc" -> JsNumber(3), "dd" -> JsNumber(4)))
      encryptResponse.fields("parent").asJsObject.fields("do_encrypt_hash").isInstanceOf[JsString] should be(true)
    }

    "expect encrypt to correctly decrypt all fields in the message marked for encrypt" in {
      encryptorActor ! Plaintext(sourceObject)
      val encryptResponse = expectMsgType[JsObject]

      encryptorActor ! Ciphertext(encryptResponse)
      val decryptResponse = expectMsgType[JsObject]

      decryptResponse.fields("no_encrypt") should be(JsString("a"))
      decryptResponse.fields("no_encrypt_hash") should be(JsObject("aa" -> JsNumber(1), "bb" -> JsNumber(2)))
      decryptResponse.fields("do_encrypt") should be(JsString("b"))
      decryptResponse.fields("do_encrypt_hash") should be(JsObject("aa" -> JsNumber(1), "bb" -> JsNumber(2)))
      decryptResponse.fields("parent").asJsObject.fields("do_encrypt_hash") should be(JsObject("aa" -> JsNumber(1), "bb" -> JsNumber(2)))
      decryptResponse.fields("parent").asJsObject.fields("no_encrypt_hash") should be(JsObject("cc" -> JsNumber(3), "dd" -> JsNumber(4)))
    }

    "expect decrypt of invalid ciphertext to result in an exception" in {
      EventFilter[IllegalArgumentException](occurrences = 1) intercept {
        encryptorActor ! Ciphertext(sourceObject)
      }
    }
  }
}
