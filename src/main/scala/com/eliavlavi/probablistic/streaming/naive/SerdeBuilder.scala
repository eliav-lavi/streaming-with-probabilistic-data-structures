package com.eliavlavi.probablistic.streaming.naive

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}

object SerdeBuilder {
  implicit def genericSerde[A]: Serde[A] =
    new Serde[A] {
      override def serializer(): Serializer[A] =
        (topic: String, data: A) => {
          val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
          val oos = new ObjectOutputStream(stream)
          oos.writeObject(data)
          oos.close()
          stream.toByteArray
        }

      override def deserializer(): Deserializer[A] =
        (topic: String, data: Array[Byte]) => {
          val ois = new ObjectInputStream(new ByteArrayInputStream(data))
          val value = ois.readObject
          ois.close()
          value.asInstanceOf[A]
        }
    }
}
