# Architecture

Changestream implements a custom [deserializer](https://github.com/shyiko/mysql-binlog-connector-java#controlling-event-deserialization) for binlog change events, and uses Akka Actors to perform several transformation steps to produce a formatted (pretty) JSON representation of the changes.

The architecture of Changestream is designed so that event format and destination is easily extended with custom Formatter, Emitter and PositionSaver classes.

It is also designed with performance in mind. By separating the various steps of event deserialization, column information, formatting/encrypting, and emitting, it is easy to configure Akka such that these steps can be performed in different threads or even on different machines via Akka Remoting.

## Application
### ChangeStream
The `ChangeStream` object is an instance of `scala.App`, and is the entry point into the application. It's responsibilities are minimal: it loads the `BinaryLogClient` configuration, configures and registers the `ChangeStreamEventListener`, and signals the `BinaryLogClient` to connect and begin processing events.

### ChangeStreamEventListener / ChangeStreamEventDeserializer
The `ChangeStreamEventListener` and `ChangeStreamEventDeserializer` handles the `BinaryLogClient`'s `onEvent` method, and:

- Converts the java objects that represent binlog event data into beautiful, easy to use [case classes](http://docs.scala-lang.org/tutorials/tour/case-classes.html). *See the `changestream.events` package for a list of all of the case classes.*
- Filters mutation events based on the configurable allowlist or blocklist
- Provides database, table name and sql query information to mutation events
- Dispatches events to the various actors in the system.

## The Actor System
### TransactionActor
The `TransactionActor` is responsible for maintaining state about
[MySQL Transactions](http://dev.mysql.com/doc/refman/5.7/en/sql-syntax-transactions.html) that may be in progress, and
provides the `JsonFormatterActor` with transaction metadata that can be appended to the JSON payload.

### ColumnInfoActor
Arguably the most complicated actor in the system, it is responsible for fetching primary key information, column names,
and data types from MySQL via a sidechannel (standard MySQL client connection). This information is used by the
`JsonFormatterActor` to provide the changed data in a nice human-readable format.

### JsonFormatterActor
The `JsonFormatterActor` is responsible for taking in information from all of the upstream actors, and emitting a JSON
formatted string. Encryption can be optionally enabled, in which case the formatted mutation
[JsObject](https://github.com/spray/spray-json#usage) is routed through the EncryptorActor before being formatted and
sent to the emitter.

### EncryptorActor
The `EncryptionActor` provides optional encryption for the change event payloads. The `Plaintext` message is a case
class that accepts a `JsObject` to encrypt. Each of the fields specified in the `changestream.encryptor.encrypt-fields`
setting is stringified via the `compactPrint` method, encrypted, base64 encoded, and finally wrapped in a `JsString`
value. The new `JsObject` with encrypted fields is then returned to `sender()`.

### SnsActor
The `SnsActor` manages a pool of connections to Amazon SNS, and emits formatted change events to a configurable
SNS topic. This publisher is ideal if you wish to leverage SNS for
[Fan-out](http://docs.aws.amazon.com/sns/latest/dg/SNS_Scenarios.html) to many queues.

### SqsActor
The `SqsActor` manages a pool of connections to Amazon SQS, and emits formatted change events to a configurable SQS
queue. This publisher is ideal if you intend to have a single service consuming events from Changestream.

### S3Actor
The `S3Actor` buffers formatted change events, and emits them in batches to S3 on a configurable interval and batch
size. This publisher can be used with Amazon Athena to easily access and query an audit log of database changes.

### StdoutActor
The `StdoutActor` is primarily included for debugging and example purposes, and simply prints the stringified mutation
to `STDOUT`. If you intend to create a custom emitter, this could be a good place to start.

### MyNewProducerActor
You probably see where this is going. More producer actors are welcome! They couldn't be easier to write, since all the
heavy lifting of creating a JSON string is already done!
```
package changestream.actors

import akka.actor.Actor
import org.slf4j.LoggerFactory

class MyNewProducerActor extends Actor {
  protected val log = LoggerFactory.getLogger(getClass)

  def receive = {
    case message: String =>
      log.debug(s"Received message: ${message}")
      // Do cool stuff

    case _ =>
      log.error("Received invalid message")
      sender() ! akka.actor.Status.Failure(new Exception("Received invalid message"))
  }
}

```

### PositionSaver
The PositionSaver actor implements a basic file-based persistence for the last successful position saved. It can be
configured to save the current position every *N* events, and/or every *N* milliseconds. When system throughput is high
you may wish to save the position less often to avoid excessive system I/O. ChangeStream will always attempt to stop
gracefully when sent a TERM signal, meaning no duplicates would be emitted. In the worst case, if ChangeStream is
stopped unexpectedly, events processed since the last position save would be re-emitted.
