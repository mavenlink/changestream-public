val nextRelease = "0.4.0"
val scalaVer = "2.11.11"

lazy val projectInfo = Seq(
  name := "changestream",
  organization := "yourorg",
  maintainer := "email <youremail@example.com>",
  packageSummary := "Changestream",
  packageDescription := "A stream of changes for MySQL built on Akka"
)

lazy val changestream = (project in file(".")).
  enablePlugins(GitVersioning, GitBranchPrompt).
  enablePlugins(JavaAppPackaging).
  enablePlugins(sbtdocker.DockerPlugin).
  settings(projectInfo: _*).
  settings(Testing.settings: _*).
  configs(Testing.configs: _*).
  settings(
    scalaVersion := scalaVer,
    scalacOptions := Seq("-deprecation", "-unchecked", "-feature"),
    // sbt-git
    git.baseVersion := nextRelease,
    git.formattedShaVersion := git.gitHeadCommit.value map {sha =>
      s"${nextRelease}-" + sys.props.getOrElse("version", default = s"$sha")
    },
    // package settings
    exportJars := true,
    debianPackageDependencies in Debian ++= Seq("oraclejdk"),
    mappings in Universal ++= Seq(
      ((resourceDirectory in Compile).value / "application.conf") -> "conf/application.conf",
      ((resourceDirectory in Compile).value / "logback.xml") -> "conf/logback.xml"
    ),
    // docker settings
    dockerfile in docker := {
      val jarFile = Keys.`package`.in(Compile, packageBin).value
      val classpath = (managedClasspath in Compile).value
      val confpath = (resourceDirectory in Compile).value.listFiles.toSeq
      val mainclass = mainClass.in(Compile, packageBin).value.get
      val lib = "/app/lib"
      val conf = "/app/conf"
      val jarTarget = "/app/" + jarFile.name

      new Dockerfile {
        from("openjdk:8-jre")

        // Copy all dependencies to 'libs' in the staging directory
        (classpath.files).foreach { depFile =>
          stageFile(depFile, file(lib) / depFile.name)
        }
        addRaw(lib, lib)

        // Copy all configs to 'conf' in the staging directory
        (confpath.get).foreach { confFile =>
          stageFile(confFile, file(conf) / confFile.name)
        }
        addRaw(conf, conf)

        // Add the generated jar file
        add(jarFile, jarTarget)

        run("bash", "-c", "curl -O https://download.newrelic.com/newrelic/java-agent/newrelic-agent/current/newrelic-java.zip && unzip newrelic-java.zip && rm newrelic-java.zip")

        // Set the entry point to start the application using the main class
        entryPoint(
          "java",
          "-classpath", s"$lib/*:$jarTarget",
          "-Dlogback.configurationFile=/app/conf/logback.xml",
          "-Dconfig.file=/app/conf/application.conf"
        )

        // By default use the main class as the cmd. Allows us to optionally add to the java command line by overriding the CMD
        // Example: CMD ["-Xms1024m", "changestream.ChangeStream"]
        cmd(mainclass)
      }
    },
    libraryDependencies ++= Dependencies.libraryDependencies,
    resolvers ++= Resolvers.list
  )
