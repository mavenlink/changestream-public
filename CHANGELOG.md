# Changestream Changelog


## 0.4.0 (2018-12-18)

- Added Kamon's stats collector to capture detailed stats on Changestream operations, including changes per table, change size, timing information, and messages in flight.
- Automatically re-connect binlog client after `TimeoutException`, where previously only `IOException` was retried.
- Log allowlist and blocklist configurations on multiple lines during startup to avoid triggering OSSEC's log length monitor.


## 0.3.1 (2018-08-06)

- Fixed regression in SQL string length limiting


## 0.3.0 (2018-08-05)

- Added PositionSaver to persist the last emitted position for graceful restarts
- Allow starting position to be overridden via an environment variable `OVERRIDE_POSITION`
- Allow restriction of the captured SQL string length via the `SQL_CHARACTER_LIMIT` environment variable


## 0.2.3 (2017-09-06)

- Optionally read the `changesteam.include-data` setting from the `INCLUDE_DATA` environment variable.
- Added optional pretty printing of JSON via the `changestream.pretty-print` setting (overridden optionally by the `PRETTY_PRINT` env).
- S3 Emitter will now separate changes with a newline character rather than [...,...] for compatibility with JsonSerDe
- Introduced a custom actor supervisor strategy to provide better error handling
- General improvements to error handling


## 0.2.2 (2017-08-24)

- Use `StdoutActor` as the default emitter actor, making it easier to get started and test changestream.
- Add the S3 Emitter Actor, with configurable `AWS_S3_BUCKET`, `AWS_S3_KEY_PREFIX`, `AWS_S3_BATCH_SIZE`, and `AWS_S3_FLUSH_TIMEOUT`
- Fixed a bug where the `AWS_REGION` environment variable was being ignored
