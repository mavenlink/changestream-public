# The Bespoke Setup

Changestream requires MySQL, and requires that [row-based replication](https://dev.mysql.com/doc/refman/5.7/en/replication-formats.html)
be enabled on your host. Here is a barebones configuration that is suitable for development:

## [Binary Log File Position Based Replication](http://dev.mysql.com/doc/refman/5.7/en/binlog-replication-configuration-overview.html)

For query info in event stream be sure to include [binlog-rows-query-log-events](https://dev.mysql.com/doc/refman/5.6/en/replication-options-binary-log.html#option_mysqld_binlog-rows-query-log-events) option

```
[mysqld]
log-bin=mysql-bin
binlog_format=row
binlog_rows_query_log_events=1 # Optional (MySQL 5.6+ only): to include the original query associated with a mutation event
server-id=952 # must be different than the same setting in application.conf
expire_logs_days=1
```

## [Replication with Global Transaction Identifiers](http://dev.mysql.com/doc/refman/5.7/en/replication-gtids.html) (MySQL 5.6+)
```
[mysqld]
log-bin=mysql-bin
gtid_mode=on
log-slave-updates
enforce-gtid-consistency
binlog_format=row
binlog_rows_query_log_events # Optional: if you want to obtain the original query associated with the mutation events
server-id=952 # must be different than the same setting in application.conf
expire_logs_days=1
```

## [Setting up a Replication User](https://dev.mysql.com/doc/refman/5.7/en/replication-howto-repuser.html)
Changestream connects to the master using a user name and password, so there must be
a user account on the master with the REPLICATION SLAVE privilege.

Further, in order to allow changestream to fetch column metadata, the user account
must have SELECT permissions on all user databases and tables.

```
CREATE USER 'changestream'@'%.mydomain.com' IDENTIFIED BY 'changestreampass';
GRANT REPLICATION CLIENT ON *.* TO 'changestream'@'%.mydomain.com';
GRANT REPLICATION SLAVE ON *.* TO 'changestream'@'%.mydomain.com';
GRANT SELECT ON *.* TO 'changestream'@'%.mydomain.com';
```
