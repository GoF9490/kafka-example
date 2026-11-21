package gof.example.kafka.configs;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConsumerConfig {
    @Value("${spring.kafka.bootstrap-servers}")
    private String kafkaServer;

    @Value("${spring.kafka.security.protocol}")
    private String securityProtocol;

    @Value("${spring.kafka.properties.sasl.mechanism}")
    private String saslMechanism;

    @Value("${spring.kafka.properties.sasl.jaas.config}")
    private String saslJaasConfig;

    @Bean
    public ConsumerFactory<String, Object> consumerFactory(KafkaProperties kafkaProperties) {
        // 수동 설정 방식. application.yaml 설정을 무시함.
        /*
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        config.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol);
        config.put(SaslConfigs.SASL_MECHANISM, saslMechanism);
        config.put(SaslConfigs.SASL_JAAS_CONFIG, saslJaasConfig);
         */

        // application.yaml 설정을 KafkaProperties 클래스로 자동으로 불러와 사용하는 방식.
        Map<String, Object> config = new HashMap<>(kafkaProperties.buildConsumerProperties());

        return new DefaultKafkaConsumerFactory<>(config);
    }

    // 생성자를 직접 호출하여 할당하는 방식.
    /*
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListener() {
        ConcurrentKafkaListenerContainerFactory<String, String> listener
                = new ConcurrentKafkaListenerContainerFactory<>();
        listener.setConsumerFactory(consumerFactory());
        // 수동 커밋을 위한 설정. 자세한 설명은 application.yaml 참조
        listener.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        return listener;
    }
     */

    // 위의 생성자로 생성된 ConsumerFactory를 자동으로 인자로 받아 사용하는 방식.
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListener(
            ConsumerFactory<String, Object> consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, String> listener =
                new ConcurrentKafkaListenerContainerFactory<>();
        listener.setConsumerFactory(consumerFactory);
        listener.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        return listener;
    }
}
