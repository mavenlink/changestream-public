package changestream.helpers

class Emitter extends Base {
  val (messageNoJson, _, _) = Fixtures.mutationWithInfo("insert", 1, 1, false, true, 1)
  val message = messageNoJson.copy(nextPosition = "binlog-001.log:1", formattedMessage = Some("{json:true}"))
  val INVALID_MESSAGE = 0
}
