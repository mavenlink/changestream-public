## Binlog Event Routing

The following [MySQL binlog](https://dev.mysql.com/doc/internals/en/binary-log.html)
events are dispatched by the `ChangeStreamEventListener`:

#### TABLE_MAP
Used for row-based binary logging. This event precedes each row
operation event. It maps a table definition to a number, where the
table definition consists of database and table names and column
definitions. The purpose of this event is to enable replication when
a table has different definitions on the master and slave. Row
operation events that belong to the same transaction may be grouped
into sequences, in which case each such sequence of events begins
with a sequence of `TABLE_MAP_EVENT` events: one per table used by
events in the sequence.

[`TABLE_MAP`](https://dev.mysql.com/doc/internals/en/table-map-event.html)
events are used by the `ChangeStreamEventDeserializer` to deserialize `MutationEvent`s
that include `database` and `tableName` info.

#### ROWS_QUERY
Contains the SQL query that was executed to produce the row events
immediately following this event.

[`ROWS_QUERY`](https://dev.mysql.com/doc/internals/en/rows-query-event.html)
events are used by the `ChangeStreamEventDeserializer` to deserialize `MutationEvent`s
that include the event's `sql` query string.

*Note: In order to include sql with the mutation events, you must enable the
`binlog_rows_query_log_events` setting in `my.cnf`.

#### QUERY
[`QUERY`](https://dev.mysql.com/doc/internals/en/query-event.html) events are converted into one of several objects:

- `AlterTableEvent` is delivered to the `ColumnInfoActor` *(signaling that the table's cache should be expired and refreshed)*
- `BeginTransaction` is delivered to the `TransactionActor`
- `CommitTransaction` is delivered to the `TransactionActor`
- `RollbackTransaction` is delivered to the `TransactionActor`

####ROWS_EVENT (aka Mutations)
Mutations (i.e. Inserts, Updates and Deletes) can be represented by several binlog
events, together referred to as
[`ROWS_EVENT`](https://dev.mysql.com/doc/internals/en/rows-event.html):

- `WRITE_ROWS` and `EXT_WRITE_ROWS` are converted to `Insert` and delivered to the `TransactionActor`.
- `UPDATE_ROWS` and `EXT_UPDATE_ROWS` are converted to `Update` and delivered to the `TransactionActor`.
	- *Note: Update events are different from Insert and Delete in that they contain both the old and the new values for the record.*
- `DELETE_ROWS` and `EXT_DELETE_ROWS` are converted to `Delete` and delivered to the `TransactionActor`.

####XID
Generated for a commit of a transaction that modifies one or more tables of an XA-capable
storage engine. Normal transactions are implemented by sending a `QUERY_EVENT` containing a
`BEGIN` statement and a `QUERY_EVENT` containing a `COMMIT` statement (or a `ROLLBACK` statement
if the transaction is rolled back).

`XID_EVENT` events, indicative of the end of a transaction, are converted to
`CommitTransaction` and delivered to the `TransactionActor`.

####GTID
Written to the binlog prior to the mutations in it's transaction, `GTID` event contains the gtid of the
transaction, and is added to each mutation event to allow consumers to group events by transaction (if desired).

`GTID` events are converted to `GtidEvent` and delivered to the `TransactionActor`.
