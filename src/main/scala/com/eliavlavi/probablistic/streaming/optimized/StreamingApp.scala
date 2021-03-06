package com.eliavlavi.probablistic.streaming.optimized

import java.time.Duration
import java.util.Properties

import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.Serdes._
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import com.eliavlavi.probablistic.streaming.optimized.HLLSerde.hllSerde

object StreamingApp extends App {
  val props = new Properties()
  props.put(StreamsConfig.APPLICATION_ID_CONFIG, "optimized-streaming-app")
  props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:39092")

  val streamsApp = new KafkaStreams(TopologyBuilder.build, props)

  streamsApp.start()
  println("streaming app is up!")

  sys.ShutdownHookThread {
    streamsApp.close(Duration.ofSeconds(10))
    println("goodbye!")
  }
}
