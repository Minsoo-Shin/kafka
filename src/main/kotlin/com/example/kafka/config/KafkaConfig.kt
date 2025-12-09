package com.example.kafka.config

import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer
import java.util.Properties

object KafkaConfig {
    // 환경에 따라 브로커 주소가 바뀌면 여기만 고치면 됩니다.
    private const val BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093,localhost:9094"

    /**
     * 기본 프로듀서 설정을 생성합니다.
     * @param extraConfig 추가로 덮어쓸 설정 (예: linger.ms, batch.size 등)
     */
    fun getProducerProps(extraConfig: Map<String, Any> = emptyMap()): Properties {
        val props = Properties()

        // 1. 필수 공통 설정
        props[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = BOOTSTRAP_SERVERS
        props[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java.name
        props[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java.name

        // 2. 기본값 설정
        props[ProducerConfig.ACKS_CONFIG] = "1"

        // 3. 데모별 특수 설정 덮어쓰기 (핵심)
        extraConfig.forEach { (key, value) ->
            props[key] = value
        }

        return props
    }
}