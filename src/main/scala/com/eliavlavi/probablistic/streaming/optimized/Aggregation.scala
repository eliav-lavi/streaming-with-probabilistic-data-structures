package com.eliavlavi.probablistic.streaming.optimized

import com.eliavlavi.probablistic.streaming.optimized.TopologyBuilder.{Hashtag, Post, User}
import com.twitter.algebird.{HLL, HyperLogLogMonoid}

object Aggregation {
  private val hllMonoid: HyperLogLogMonoid = new HyperLogLogMonoid(bits = 8)

  val init: HLL = hllMonoid.zero
  val aggregate: (User, Post, HLL) => HLL = (_, post, currentHLL) =>
    currentHLL + hllMonoid.sum(
      extractHashtags(post).map(hashtag => hllMonoid.create(hashtag.toCharArray.map(_.toByte)))
    )

  private def extractHashtags(post: Post): Set[Hashtag] = {
    val Pattern = """(#\w+)""".r
    Pattern.findAllIn(post).toSet
  }
}
