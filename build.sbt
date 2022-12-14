import sbt._
import sbt.Keys._

Global / onChangedBuildSource := ReloadOnSourceChanges

inThisBuild(
  List(
    organization := "net.cilib",
    homepage     := Some(url("https://cilib,net")),
    licenses     := List("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0")),
    developers   := List(
      Developer(
        "gpampara",
        "Gary Pamparà",
        "",
        url("http://gpampara.github.io")
      )
    ),
    scmInfo      := Some(
      ScmInfo(url("https://github.com/ciren/cilib/"), "scm:git:git@github.com:ciren/cilib.git")
    )
  )
)

scalaVersion := "2.13.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.3.0",
  "org.apache.spark" %% "spark-core" % "3.3.0",
  "dev.zio" %% "zio-cli" % "0.2.7"
)

run / fork := true
