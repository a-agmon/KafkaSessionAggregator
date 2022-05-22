ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.13.8"

name := "SessionAggregator"

libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka-streams" % "3.1.0",
  "org.apache.kafka" % "kafka-clients" % "3.1.0",
  "org.apache.kafka" %% "kafka-streams-scala" % "3.1.0",
  // https://mvnrepository.com/artifact/io.circe/circe-core
  "io.circe" %% "circe-core" % "0.15.0-M1",
  "io.circe" %% "circe-generic" % "0.15.0-M1",
  "io.circe" %% "circe-parser" % "0.15.0-M1",
  // logging
  "org.slf4j" % "slf4j-api" % "1.7.5",
  "org.slf4j" % "slf4j-simple" % "1.7.5",

)