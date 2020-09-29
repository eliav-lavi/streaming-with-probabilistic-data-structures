package com.eliavlavi.probablistic.streaming.naive

import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.scala.{ByteArrayKeyValueStore, StreamsBuilder}
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.Serdes._
import org.apache.kafka.streams.scala.kstream.Materialized

object TopologyBuilder {
  type User = String
  type Post = String
  type Hashtag = String

  def build(implicit
      materialized: Materialized[User, Set[Hashtag], ByteArrayKeyValueStore]
  ): Topology = {
    val builder = new StreamsBuilder()
    builder
      .stream[User, Post]("posts")
      .groupByKey
      .aggregate(Set.empty[Hashtag])((_, post, hashtags) => hashtags ++ extractHashtags(post))
      .toStream
      .map((user, hashtags) => (user, hashtags.size))
      .to("user-hashtag-counts")

    builder.build()
  }

  private def extractHashtags(post: Post): Set[Hashtag] = {
    val Pattern = """(#\w+)""".r
    Pattern.findAllIn(post).toSet
  }
}
