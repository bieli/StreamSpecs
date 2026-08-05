ThisBuild / version      := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "3.3.4"
ThisBuild / organization := "com.streamspecs"

val catsEffectV = "3.5.7"
val fs2V        = "3.11.0"
val fs2KafkaV   = "3.5.1"
val circeV      = "0.14.10"
val pureconfigV = "0.17.8"
val munitV      = "1.0.2"
val munitCEV    = "2.0.0"
val prometheusV = "0.16.0"
val logbackV    = "1.5.12"

lazy val commonSettings = Seq(
  scalacOptions ++= Seq("-deprecation", "-feature", "-unchecked", "-Wunused:all"),
  testFrameworks += new TestFramework("munit.Framework")
)

lazy val root = (project in file("."))
  .aggregate(core, examples)
  .settings(
    name           := "stream-specs",
    publish / skip := true
  )

lazy val core = (project in file("core"))
  .settings(
    commonSettings,
    name := "stream-specs-core",
    libraryDependencies ++= Seq(
      "org.typelevel"         %% "cats-effect"               % catsEffectV,
      "co.fs2"                %% "fs2-core"                  % fs2V,
      "co.fs2"                %% "fs2-io"                    % fs2V,
      "com.github.fd4s"       %% "fs2-kafka"                 % fs2KafkaV,
      "io.circe"              %% "circe-core"                % circeV,
      "io.circe"              %% "circe-generic"             % circeV,
      "io.circe"              %% "circe-parser"              % circeV,
      "com.github.pureconfig" %% "pureconfig-core"           % pureconfigV,
      "com.github.pureconfig" %% "pureconfig-generic-scala3" % pureconfigV,
      "io.prometheus"          % "simpleclient"              % prometheusV,
      "io.prometheus"          % "simpleclient_hotspot"      % prometheusV,
      "io.prometheus"          % "simpleclient_httpserver"   % prometheusV,
      "ch.qos.logback"         % "logback-classic"           % logbackV % Runtime,
      "org.scalameta"         %% "munit"                     % munitV   % Test,
      "org.typelevel"         %% "munit-cats-effect"         % munitCEV % Test
    )
  )

lazy val examples = (project in file("examples"))
  .dependsOn(core)
  .enablePlugins(JavaAppPackaging)
  .settings(
    commonSettings,
    name                 := "stream-specs-examples",
    publish / skip       := true,
    Compile / mainClass  := Some("com.streamspecs.examples.iot.IoTStreamSpecsApp"),
    Compile / run / fork := true,
    libraryDependencies ++= Seq(
      "ch.qos.logback" % "logback-classic" % logbackV
    )
  )

addCommandAlias("fmt", "scalafmtAll; scalafmtSbt")
addCommandAlias("fmtCheck", "scalafmtCheckAll; scalafmtSbtCheck")
addCommandAlias("ci", "fmtCheck; core/test; examples/compile")
