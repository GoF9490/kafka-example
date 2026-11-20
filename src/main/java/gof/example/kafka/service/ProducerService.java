package gof.example.kafka.service;

import gof.example.kafka.dtos.MessageDto;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.ObjectMapper;

@Service
@Transactional
public class ProducerService {
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public ProducerService(KafkaTemplate<String, Object> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    public void messageCreate(MessageDto dto) {
        System.out.println(dto);
        String memberData = objectMapper.writeValueAsString(dto);
        sendKafkaMessage("member-topic", memberData);
    }

    public void messageCreateKey(MessageDto dto, String key) {
        System.out.println(dto);
        String data = objectMapper.writeValueAsString(dto);
        sendKafkaMessage("member-topic", key, data);
    }

    public void sendKafkaMessage(String topic, String message) {
        kafkaTemplate.send(topic, message);
    }

    public void sendKafkaMessage(String topic, String key, String message) {
        kafkaTemplate.send(topic, key, message);
    }
}
