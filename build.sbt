name := "kafka-examples"

version := "0.1.0"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka_2.11" % "0.8.2.2",
  "org.scalatest" % "scalatest_2.11" % "2.2.4" % "test",
  "com.typesafe" % "config" % "1.3.0",
  "org.apache.curator" % "curator-test" % "2.7.0"
)
