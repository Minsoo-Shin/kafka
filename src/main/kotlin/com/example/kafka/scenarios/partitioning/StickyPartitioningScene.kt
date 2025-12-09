package com.example.kafka.scenarios.partitioning

import com.example.kafka.config.KafkaConfig
import com.example.kafka.util.TopicManager.ensureTopicExists
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord

fun main() {
    val topicName = "sticky-test-topic"

    // 0. 토픽 자동 생성 (AdminClient 활용)
    ensureTopicExists(
        topicName = topicName,
        numPartitions = 3
    )

    println(">>> [테스트] Kafka Sticky Partitioning 시작")

    // 1. 프로듀서 설정
    val props = KafkaConfig.getProducerProps(mapOf(
//        ProducerConfig.LINGER_MS_CONFIG to 20, // default 0이기 때문에 Sticky 배치 효과를 위해 설정
//        ProducerConfig.BATCH_SIZE_CONFIG to 32 * 1024 // default 16384
    ))


    val producer = KafkaProducer<String, String>(props)

    try {
        println(">>> 메시지 전송 시작 (Key=null)...")

        for (i in 0 until 1000) {
            // Key를 null로 설정
            val record = ProducerRecord<String, String>(topicName, null, "msg-$i")

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
