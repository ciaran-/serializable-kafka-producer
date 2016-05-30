package com.softbrew.kafka

import java.util.Properties

import kafka.javaapi.producer.Producer
import kafka.producer.{KeyedMessage, ProducerConfig}


/*
 ** Simple App that publishes some dummy data to a test kafka topic.
 */
class SerializableKafkaProducer extends Serializable {

  /**
    * Set up the Kafka configuration.
    *
    * @return a Kafka Producer
    */
  def publishToTopic(brokers: String, topic: String, content: String): Unit = {
    val props = new Properties()
    props.put("metadata.broker.list", brokers)
    props.put("serializer.class", "kafka.serializer.StringEncoder")

    val kafkaConfig = new ProducerConfig(props)
    val sender =  new Producer[String, String](kafkaConfig)

    val msg = new KeyedMessage[String, String](topic, content)
    sender.send(msg)
  }
}