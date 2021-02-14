package com.gin.flink.scala.stream.source

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import scala.io.Source

object KafkaProducer {
  def main(args: Array[String]): Unit = {
    //配置连接kafka的信息  Properties
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "node01:9092,node02:9092,node03:9092")
    properties.setProperty("key.serializer", classOf[StringSerializer].getName)
    properties.setProperty("value.serializer", classOf[StringSerializer].getName)

    //创建一个kafka生产者对象
    val producer = new KafkaProducer[String, String](properties)

    //测试用API, 不要轻易调用getLines(会一次性加载所有)
    val iterator = Source.fromFile("./data/carFlow_all_column_test.txt").getLines()
    // shell中消费查看生产的消息:
    // kafka-console-consumer.sh --bootstrap-server node01:9092  --topic flk-kafka --property print.key=true
    for (i <- 1 to 100) {
      for (elem <- iterator) {
        println(elem)
        //kv mq
        val splits = elem.split(",")
        val monitorId = splits(0).replace("'", "")
        val carId = splits(2).replace("'", "")
        val timestamp = splits(4).replace("'", "")
        val speed = splits(6)
        val builder = new StringBuilder
        val info = builder.append(monitorId + "\t").append(carId + "\t").append(timestamp + "\t").append(speed)
        producer.send(new ProducerRecord[String, String]("flink-kafka", i + "", info.toString))
        Thread.sleep(200)
      }
    }
  }
}
