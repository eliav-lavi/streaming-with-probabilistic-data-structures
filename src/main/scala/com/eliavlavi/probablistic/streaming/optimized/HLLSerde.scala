package com.eliavlavi.probablistic.streaming.optimized

import com.twitter.algebird.{HLL, HyperLogLog}
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}

object HLLSerde {
  implicit val hllSerde: Serde[HLL] = new Serde[HLL] {
    override def serializer(): Serializer[HLL] = (_: String, data: HLL) => HyperLogLog.toBytes(data)
    override def deserializer(): Deserializer[HLL] =
      (topic: String, data: Array[Byte]) => HyperLogLog.fromBytes(data)
  }
}