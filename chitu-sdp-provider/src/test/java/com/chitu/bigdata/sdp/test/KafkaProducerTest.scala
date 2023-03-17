package com.chitu.bigdata.sdp.test

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.util.Properties

/**
 * @author sutao
 * @create 2021-10-26 10:57
 */
object KafkaProducerTest {

  def main(args: Array[String]): Unit = {

    //System.setProperty("java.security.auth.login.config", "D:\\tmp\\kafka_client_jaas_sdp_flink.conf")

    val properties = new Properties
    properties.put("bootstrap.servers", "******")
    properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.put("acks", "all")
//    properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT")
//    properties.put(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-256")
//    properties.put("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"sdp_flink\" password=\"******\";")
    //properties.put("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"writeonly\" password=\"******\";")
    val kafkaProducer: KafkaProducer[String, String] = new KafkaProducer[String, String](properties)
    for(i <- 1 to 99999){
      val value = kafkaProducer.send(new ProducerRecord[String, String]("test_group_offset", "{\"id\":\"590951\",\"score\":1121}"))
      val metadata = value.get()
//      println(metadata.partition())
      println(metadata.offset())
//      println(metadata.timestamp())
//      println("写入成功")
    }
  }

}
