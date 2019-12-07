package com.zouls.hotItems

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object MyKafkaProducer {
  def main(args: Array[String]): Unit = {
    writeToKafka("zouls-test")
  }

  def writeToKafka(topic: String): Unit = {
    val properties = new Properties()
    properties.put("bootstrap.servers", "192.168.1.110:9092")
    properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    // 定义一个kafka producer
    val producer = new KafkaProducer[String, String](properties)
    // 从文件中读取数据，发送
    val bufferedSource = io.Source.fromFile("file:///C:/Users/zouls/Projects/flink/user-behavior-analysis/hot-items/src/main/resources/UserBehavior.csv")
    for (line <- bufferedSource.getLines()) {
      val record = new ProducerRecord[String, String](topic, line)
      producer.send(record)
    }
    producer.close()
  }
}
