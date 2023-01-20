import sbt._

object Dependencies {
  // Versions
  lazy val akkaVersion = "2.5.14"
  lazy val awsVersion = "1.11.172"
  lazy val sprayVersion = "1.3.4"

  val libraryDependencies = Seq(
    // application
    "com.typesafe" % "config" % "1.3.1",
    "org.slf4j" % "slf4j-api" % "1.7.25",
    "ch.qos.logback" % "logback-classic" % "1.1.11",
    // akka actor system
    "com.typesafe.akka" %% "akka-actor" % akkaVersion,
    "com.typesafe.akka" %% "akka-agent" % akkaVersion,
    "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
    // spray for http control server
    "io.spray" %% "spray-can" % sprayVersion,
    "io.spray" %% "spray-routing" % sprayVersion,
    // instrumentation
    "io.kamon" %% "kamon-core" % "1.1.3",
    "io.kamon" %% "kamon-akka-2.5" % "1.1.2",
    "io.kamon" %% "kamon-prometheus" % "1.1.1",
    "io.kamon" %% "kamon-datadog" % "1.0.0",
    "io.kamon" %% "kamon-newrelic" % "2.5.9",
    // mysql
    "com.github.shyiko" % "mysql-binlog-connector-java" % "0.13.0",
    "com.github.mauricio" %% "mysql-async" % "0.2.21",
    // json formatter
    "io.spray" %%  "spray-json" % "1.3.3",
    // event emitter
    "com.amazonaws" % "aws-java-sdk-sqs" % awsVersion,
    "com.amazonaws" % "aws-java-sdk-sns" % awsVersion,
    "com.amazonaws" % "aws-java-sdk-s3" % awsVersion,
    "com.github.dwhjames" %% "aws-wrap" % "0.12.1"
  )

  val testDependencies = Seq(
    "com.typesafe.akka" %% "akka-testkit" % akkaVersion % "it,test,bench",
    "io.spray" %% "spray-testkit" % sprayVersion % "it,test,bench",
    "org.scalatest" %% "scalatest" % "3.0.5" % "it,test,bench",
    "com.storm-enroute" %% "scalameter" % "0.8.2" % "it,test,bench"
  )
}
