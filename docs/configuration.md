# Configuration

## MySQL Configuration

Ensure that the MySQL authentication info in `src/main/resources/application.conf` is correct. You can override the defaults during development by creating a `src/main/resources/application.overrides.conf` file, which is git ignored:

```
changestream {
  mysql {
    host = "localhost"
    port = 3306
    user = "changestream"
    password = "changestreampass"
  }
}
```

You can also configure most settings using environment variables. For example, you could put the following in a Changestream init script:

```
export MYSQL_HOST=localhost
export MYSQL_PORT=3306
export MYSQL_USER=changestream
export MYSQL_PASS=changestreampass
```

## Emitter Configuration

If you would like to override the default emitter (`StdoutActor`), you can do so by setting `changestream.emitter` or the `EMITTER` environment variable to the fully qualified class name (for example, `changestream.actors.SnsActor`).

To configure the SNS emitter, you must [provide AWS credentials](http://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html#config-settings-and-precedence), and configure `changestream.aws.sns.topic`.
