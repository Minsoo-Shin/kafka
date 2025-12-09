package com.example.kafka.scenarios.partitioning

import com.example.kafka.config.KafkaConfig
import com.example.kafka.util.TopicManager.ensureTopicExists
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord

fun main() {
    val topicName = "key-based-test-topic"

    ensureTopicExists(
        topicName = topicName,
        numPartitions = 3
    )


    println(">>> [테스트] Kafka Key Based Partitioning 시작")

    val props = KafkaConfig.getProducerProps(
        mapOf(

        )
    )

    val produce = KafkaProducer<String, String>(props)

    try {
        for (i in 0 until 1000) {
            val messageGroup = i % 3
            val key = "key-$messageGroup"
            // 키별로 파티션을 고정적으로 전송됨
            // 사용 사례는 순서가 중요할 때, key based 사용. (파티션끼리는 순서 보장을 하지 않기 떄문)
            // (주의: 파티션이 늘어나는 경우, 다른 파티션으로 재배정됨)
            //
            val record = ProducerRecord(topicName, key, "msg-$i")
            produce.send(record) { metadata, exception ->
                if (exception == null) {
                    print("key: $key - [P${metadata.partition()}]")
                } else {
                    System.err.println("X")
                }
            }
        }
    } finally {
        produce.close()
        println("\n>>> 테스트 종료")
    }
}