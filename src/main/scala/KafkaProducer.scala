package es.novaquality.spark

import faker.Lorem
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import java.util.Properties

object KafkaProducer extends App {

  val kafkaProducerProps: Properties = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9093")
    props.put("key.serializer", classOf[StringSerializer].getName)
    props.put("value.serializer", classOf[StringSerializer].getName)
    props
  }

  val producer = new KafkaProducer[String, String](kafkaProducerProps)

  while (true) {
    val record = new ProducerRecord[String, String](
      "sentences",
      Lorem.sentence(1),
      Lorem.sentence(10)
    )

    val result = producer.send(record)

    println(record.value())
//    printf(
//      s"sent record(key=%s value=%s) meta(partition=%d, offset=%d)\n",
//      record.key(), record.value(), result.get().partition(), result.get().offset()
//    )
  }
}
