# The Stack

Changestream is designed to be fast, stable and massively scalable, so it is built on:

- **[Akka](http://akka.io/)** for multi-threaded, asyncronous, and 100% non-blocking processing of events, from our friends at [Lightbend](https://www.lightbend.com/)
- **[shyiko/mysql-binlog-connector-java](https://github.com/shyiko/mysql-binlog-connector-java)** by [Stanley Shyiko](https://github.com/shyiko) to consume the raw MySQL binlog (did I mention its awesome, [and stable too!](http://devs.mailchimp.com/blog/powering-mailchimp-pro-reporting))
- **[mauricio/postgresql-async](https://github.com/mauricio/postgresql-async)** Async, Netty based, database drivers for PostgreSQL and MySQL written in Scala, by [Maurício Linhares](https://github.com/mauricio)
- **[AWS Java SDK](https://aws.amazon.com/sdk-for-java/)**
- **[spray/spray-json](https://github.com/spray/spray-json)** A lightweight, clean and simple JSON implementation in Scala. Used by [Spray](http://spray.io/)/[akka-http](http://doc.akka.io/docs/akka/current/scala.html)
- **[mingchuno/aws-wrap](https://github.com/mingchuno/aws-wrap)** Asynchronous Scala Clients for Amazon Web Services by [Daniel James](https://github.com/dwhjames), maintained by [mingchuno](https://github.com/mingchuno)
