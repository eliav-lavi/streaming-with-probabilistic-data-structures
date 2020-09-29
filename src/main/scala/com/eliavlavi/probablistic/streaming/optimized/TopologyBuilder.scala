package com.eliavlavi.probablistic.streaming.optimized

import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.Serdes._
import org.apache.kafka.streams.scala.kstream.Materialized
import org.apache.kafka.streams.scala.{ByteArrayKeyValueStore, StreamsBuilder}
import com.twitter.algebird.HLL

object TopologyBuilder {
  type User = String
  type Post = String
  type Hashtag = String

  def build(implicit
      materialized: Materialized[String, HLL, ByteArrayKeyValueStore]
  ): Topology = {
    val builder = new StreamsBuilder()
    builder
      .stream[User, Post]("posts")
      .groupByKey
      .aggregate(Aggregation.init)(Aggregation.aggregate)
      .toStream
      .map((user, hll) => (user, hll.approximateSize.estimate.toInt))
      .to("user-hashtag-counts")

    builder.build()
  }
}
