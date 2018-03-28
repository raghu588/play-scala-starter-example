
name := """play-scala-starter-example"""

version := "1.0-SNAPSHOT"


lazy val root = (project in file(".")).enablePlugins(PlayScala)

resolvers += Resolver.sonatypeRepo("snapshots")

scalaVersion := "2.12.4"

crossScalaVersions := Seq("2.11.12", "2.12.4")

libraryDependencies += guice
libraryDependencies += "org.scalatestplus.play" %% "scalatestplus-play" % "3.1.2" % Test
libraryDependencies += "com.h2database" % "h2" % "1.4.196"
libraryDependencies += "org.json" % "json" % "20180130"

val elastic4sVersion = "0.90.2.8"

libraryDependencies ++= Seq(
  "com.sksamuel.elastic4s" %% "elastic4s-core" % "5.5.3",
  "com.sksamuel.elastic4s" %% "elastic4s-tcp" % "5.5.3",
  "com.sksamuel.elastic4s" %% "elastic4s-http" % "5.5.3",
  "com.sksamuel.elastic4s" %% "elastic4s-streams" % "5.5.3",
  "com.sksamuel.elastic4s" %% "elastic4s-circe" % "5.5.3",
  "org.apache.logging.log4j" % "log4j-api" % "2.9.1", // For Log4j2.xml file in resources
  "org.apache.logging.log4j" % "log4j-core" % "2.9.1",
  "com.sksamuel.elastic4s" %% "elastic4s-testkit" % "5.5.3" % "test"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
