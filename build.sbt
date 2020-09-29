name := "streaming-with-probabilistic-data-structures"

version := "0.1"

scalaVersion := "2.13.3"

val kafkaStreamsVersion = "2.6.0"

libraryDependencies ++= List[ModuleID](
  "org.apache.kafka" % "kafka-streams" % kafkaStreamsVersion,
  "org.apache.kafka" % "kafka-streams-test-utils" % kafkaStreamsVersion % Test,
  "org.apache.kafka" %% "kafka-streams-scala" % kafkaStreamsVersion,

  "com.twitter" %% "algebird-core" % "0.13.7",

  "org.scalactic" %% "scalactic" % "3.2.0",
  "org.scalatest" %% "scalatest" % "3.2.0" % "test"
)

