import sbt._
import Keys._

object AkkaPageRankBuild extends Build {

  lazy val buildSettings = Defaults.defaultSettings ++ Seq(
    // project settings
    version := "0.1.0-SNAPSHOT",
    organization := "me.juhanlol",
    scalaVersion := "2.10.4",
    // dependencies
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "2.2.1" % "test",
      "com.typesafe.akka" %% "akka-actor" % "2.3.6",
      "org.scalanlp" %% "breeze" % "0.10",
      "org.scalanlp" %% "breeze-natives" % "0.10"
    )
  )

  lazy val project = Project("akka-pagerank", file("."),
    settings = buildSettings)
}
