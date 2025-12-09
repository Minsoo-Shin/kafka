package com.example.kafka.util

import com.example.kafka.config.KafkaConfig
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.producer.ProducerConfig
import java.util.Collections
import java.util.Properties

object TopicManager {

    /**
     * 토픽이 존재하는지 확인하고, 없으면 지정된 파티션 수로 생성합니다.
     */
    fun ensureTopicExists(topicName: String, numPartitions: Int) {
        val props = Properties()
        // KafkaConfig에서 주소만 빌려옵니다.
        props[AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG] =
            KafkaConfig.getProducerProps().getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG)

        println(">>> [TopicManager] 토픽 점검 중: $topicName")

        AdminClient.create(props).use { admin ->
            val existingTopics = admin.listTopics().names().get()

            if (existingTopics.contains(topicName)) {
                println("✔ 토픽이 이미 존재합니다.")
                // (심화: 파티션 개수가 다르면 경고를 띄우는 로직을 추가할 수도 있음)
            } else {
                println("! 토픽이 없습니다. 생성합니다. (Partitions: $numPartitions)")
                val newTopic = NewTopic(topicName, numPartitions, 1.toShort())
                admin.createTopics(Collections.singleton(newTopic)).all().get()
                println("✔ 토픽 생성 완료!")
            }
        }
    }
}
