package com.eliavlavi.probablistic.streaming.naive

import java.util.Properties

import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.scala.{ByteArrayKeyValueStore, Serdes}
import org.apache.kafka.streams.state.{KeyValueStore, Stores}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import com.eliavlavi.probablistic.streaming.naive.SerdeBuilder._

import scala.jdk.CollectionConverters.MapHasAsScala

class TopologyBuilderTest extends AnyFunSpec with Matchers {
  describe("topology") {
    import org.apache.kafka.streams.StreamsConfig
    import org.apache.kafka.streams.TopologyTestDriver
    val props = new Properties()
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234")

    val materialized: Materialized[String, Set[String], ByteArrayKeyValueStore] =
      Materialized
        .as[String, Set[String]](
          Stores.inMemoryKeyValueStore("test-store")
        )
        .withValueSerde(implicitly[Serde[Set[String]]])
    val testDriver = new TopologyTestDriver(TopologyBuilder.build(materialized), props)
    val inputTopic =
      testDriver.createInputTopic("posts", Serdes.String.serializer(), Serdes.String.serializer())
    val outputTopic = testDriver.createOutputTopic(
      "user-hashtag-counts",
      Serdes.String.deserializer(),
      Serdes.Integer.deserializer()
    )

    it("counts distinct hashtags per user") {
      inputTopic.pipeInput("john.smith99", "Just trying things out")
      val state0 = outputTopic.readKeyValuesToMap().asScala.toMap
      state0("john.smith99") shouldEqual 0

      inputTopic.pipeInput("john.smith99", "Trying out this hashtag thing; #scala")
      val state1 = outputTopic.readKeyValuesToMap().asScala.toMap
      state1("john.smith99") shouldEqual 1

      inputTopic.pipeInput("john.smith99", "Gotta #love working on #streaming in #scala")
      val state2 = outputTopic.readKeyValuesToMap().asScala.toMap
      state2("john.smith99") shouldEqual 3
    }
  }
}
