# The Mutation Lifecycle

## Mutation Metadata
The `MutationWithInfo` case class is a container that holds a mutation, and contains slots for various information that will be provided to this event as it is prepared for formatting (implemented via the [`Option`](http://danielwestheide.com/blog/2012/12/19/the-neophytes-guide-to-scala-part-5-the-option-type.html) pattern).

### MutationEvent
`MutationEvent` contains information about the row or rows that changed. This field is required. All other fields in the case class are initally optional to allow the information to be provided as the event makes its way to the `JsonFormatterActor`.

A `MutationEvent` must have the following information in order to be formatted correctly:

### TransactionInfo
`TransactionInfo` contains metadata about the transaction to which the mutation belongs, or [`None`](http://www.scala-lang.org/api/2.7.0/scala/None$object.html) if the mutation is not part of a transaction.

### ColumnsInfo
`ColumnsInfo` contains information about all of the columns in the table to which the mutation belongs.

## Mutation Routing
The `MutationWithInfo` container object travels in a fixed path through the system, gathering metadata along the way.

For those familiar with [Akka Streams](http://doc.akka.io/docs/akka/current/scala/stream/stream-introduction.html), the parantheticals should help to show how events move through the actor system. *Also, for those familiar with Akka Streams, please help me migrate Changestream to Akka Streams.*

### ChangeStreamEventListener.onEvent (Source)
- Creates/Emits: `MutationWithInfo(MutationEvent, None, None, None)`

### TransactionActor (Flow)
- Receives: `MutationWithInfo(MutationEvent, None, None)`
- Emits: `MutationWithInfo(MutationEvent, TransactionInfo, None, None)` or `MutationWithInfo(MutationEvent, None, None, None)` (if there is no transaction)

### ColumnInfoActor (Flow)
- Receives: `MutationWithInfo(MutationEvent, Option[TransactionInfo], None, None)`
- Emits: `MutationWithInfo(MutationEvent, Option[TransactionInfo], ColumnsInfo, None)`

### JsonFormatterActor (Flow)
- Receives: `MutationWithInfo(MutationEvent, Option[TransactionInfo], ColumnsInfo, None)`
- Emits: `MutationWithInfo(MutationEvent, Option[TransactionInfo], ColumnsInfo, Some(String))`

#### EncryptorActor (Flow)
Optional. The `EncryptorActor` is a child actor of `JsonFormatterActor`, and if enabled, will
receive the completed `JsObject` and encrypt fields in the object based on the `changestream.encryptor.encrypt-fields` setting.

- Receives: `Plaintext(JsObject)`
- Emits: `JsObject`

### SnsActor / SqsActor / StdoutActor (Sink)
In order to enable features such as string interpolation for the configured topic name, we pass the
`MutationWithInfo` all the way through the process.
- Receives: `MutationWithInfo(MutationEvent, Option[TransactionInfo], ColumnsInfo, Some(String))`
