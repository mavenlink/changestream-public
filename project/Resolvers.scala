import sbt._

object Resolvers {
  val list = Seq(
    Resolver.mavenLocal,
    Resolver.sonatypeRepo("releases"),
    Resolver.typesafeRepo("releases"),
    "Spray Repository" at "http://repo.spray.io",
    Resolver.jcenterRepo,
    Resolver.sbtPluginRepo("sbt-plugin-releases"),
    Resolver.bintrayRepo("mingchuno", "maven"),
    Resolver.bintrayRepo("kamon-io", "sbt-plugins")
  )
}
