package es.novaquality.spark

import SparkLogLevels.setLogLevelToWarn

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies._
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Milliseconds, Seconds, StreamingContext}

object KafkaStream extends App {

  setLogLevelToWarn()

  val conf = new SparkConf().setMaster("local[1]").setAppName("KafkaStream")
  val streamingContext = new StreamingContext(conf, Milliseconds(1))

  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "localhost:9093",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "novaquality",
    "auto.offset.reset" -> "earliest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  val topics = Array("sentences")

  val stream = KafkaUtils.createDirectStream[String, String](
    streamingContext,
    PreferConsistent,
    Subscribe[String, String](topics, kafkaParams)
  )

  stream.flatMap(keyValue => keyValue.value().split(" "))
    .map(word => (word, 1))
    .reduceByKey(_ + _)
    .print(10)

  streamingContext.start()
  streamingContext.awaitTermination()
}