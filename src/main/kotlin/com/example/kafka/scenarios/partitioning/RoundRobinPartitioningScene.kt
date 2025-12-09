package com.example.kafka.scenarios.partitioning

import com.example.kafka.config.KafkaConfig
import com.example.kafka.util.TopicManager.ensureTopicExists
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RoundRobinPartitioner
import kotlin.jvm.java

fun main() {
    val topicName = "round-robin-test-topic"

   // Topic 생성
    ensureTopicExists(
        topicName = topicName,
        numPartitions = 3
    )
    println(">>> [테스트] Kafka Sticky Partitioning 시작")

   val props = KafkaConfig.getProducerProps(mapOf(
       ProducerConfig.PARTITIONER_CLASS_CONFIG to RoundRobinPartitioner::class.java.name
   ))

    val producer = KafkaProducer<String, String>(props)

    try {
        println(">>> 메시지 전송 시작 (Key=null)...")

        for (i in 0 until 1000) {
            // Key를 null로 설정
            val record = ProducerRecord<String, String>(topicName, null,  "msg-$i")

            producer.send(record) { metadata, exception ->
                if (exception == null) {
                    print("[P${metadata.partition()}] ")
                } else {
                    System.err.println("X")
                }
            }
        }

    } finally {
        producer.close()
        println("\n>>> 테스트 종료")
    }
}
