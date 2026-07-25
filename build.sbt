ThisBuild / version      := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "3.3.4"
ThisBuild / organization := "com.streamspecs"

lazy val root = (project in file("."))
  .enablePlugins(JavaAppPackaging)
  .settings(
    name := "stream-specs",
    libraryDependencies ++= Seq(
      "org.typelevel"         %% "cats-effect"               % "3.5.7",
      "co.fs2"                %% "fs2-core"                  % "3.11.0",
      "co.fs2"                %% "fs2-io"                    % "3.11.0",
      "com.github.fd4s"       %% "fs2-kafka"                 % "3.5.1",
      "io.circe"              %% "circe-core"                % "0.14.10",
      "io.circe"              %% "circe-generic"             % "0.14.10",
      "io.circe"              %% "circe-parser"              % "0.14.10",
      "com.github.pureconfig" %% "pureconfig-core"           % "0.17.8",
      "com.github.pureconfig" %% "pureconfig-generic-scala3" % "0.17.8",
      "org.typelevel"         %% "log4cats-slf4j"            % "2.7.0",
      "ch.qos.logback"         % "logback-classic"           % "1.5.12",
      "io.prometheus"          % "simpleclient"              % "0.16.0",
      "io.prometheus"          % "simpleclient_hotspot"      % "0.16.0",
      "io.prometheus"          % "simpleclient_httpserver"   % "0.16.0",
      "org.scalameta"         %% "munit"                     % "1.0.2"  % Test,
      "org.typelevel"         %% "munit-cats-effect"         % "2.0.0"  % Test
    ),
    scalacOptions ++= Seq(
      "-deprecation",
      "-feature",
      "-unchecked",
      "-Wunused:all"
    ),
    Compile / mainClass  := Some("com.streamspecs.StreamSpecsApp"),
    Compile / run / fork := true,
    testFrameworks += new TestFramework("munit.Framework")
  )

addCommandAlias("fmt", "scalafmtAll; scalafmtSbt")
addCommandAlias("fmtCheck", "scalafmtCheckAll; scalafmtSbtCheck")
addCommandAlias("ci", "fmtCheck; Test/compile; test")
