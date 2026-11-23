package gof.example.kafka.configs;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;

import java.util.HashMap;
import java.util.Map;

@Configuration
// @EnableKafkaStreams // 다중 streams 구성 시 해당 어노테이션은 붙이지 않습니다.
public class KafkaStreamsConfig {

    @Value("${custom.kafka.streams.message-count-id}")
    private String messageCountId;

    @Value("${custom.kafka.streams.duplicate-alert-id}")
    private String duplicateAlertId;

    // 단일 streams 구성
    /*
    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kStreamsConfig(KafkaProperties kafkaProperties) {
        // 1. application.yml에 적힌 spring.kafka.streams.* 및 공통 설정 로드
        Map<String, Object> config = new HashMap<>(kafkaProperties.buildStreamsProperties());

        // 2. 코드로 추가하거나 덮어쓸  설정 입력
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        return new KafkaStreamsConfiguration(config);
    }
     */

    // 다중 streams 구성
    @Bean("messageCountStreamsBuilder")
    public StreamsBuilderFactoryBean messageCountStreamsBuilder(KafkaProperties kafkaProperties) {
        Map<String, Object> props = new HashMap<>(kafkaProperties.buildStreamsProperties());
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, messageCountId);
        return new StreamsBuilderFactoryBean(new KafkaStreamsConfiguration(props));
    }

    @Bean("duplicateAlertStreamsBuilder")
    public StreamsBuilderFactoryBean duplicateAlertStreamsBuilder(KafkaProperties kafkaProperties) {
        Map<String, Object> props = new HashMap<>(kafkaProperties.buildStreamsProperties());
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, duplicateAlertId);
        return new StreamsBuilderFactoryBean(new KafkaStreamsConfiguration(props));
    }
}
