package gof.example.kafka.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@Transactional
public class ConsumerService {

    @Autowired
    private ProducerService producerService;

    @KafkaListener(
            topics = "message-topic",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListener"
    )
    public void consumeMessageTopic(
            String message,
            Acknowledgment ack
    ) {
        try {
            System.out.println("message: " + message);

            System.out.println("Commit");
            ack.acknowledge(); //수동 커밋
        } catch (Exception e) {
            e.printStackTrace();
            // DLQ 예시: 실패 메시지를 별도 토픽으로 발행
            producerService.sendKafkaMessage("message-topic-dlq", message);
            ack.acknowledge(); //수동 커밋
        }
    }

    @KafkaListener(
            topics = "message-topic",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListener"
    )
    public void consumeMessageTopic(
            @Header(KafkaHeaders.RECEIVED_KEY) String key,
            String message,
            Acknowledgment ack
    ) {
        try {
            System.out.println("message: " + message);

            if (key.equals("1"))
                throw new IllegalAccessException("Exception: key is 1");

            System.out.println("Commit");
            ack.acknowledge(); //수동 커밋
        } catch (Exception e) {
            e.printStackTrace();
            // DLQ 예시: 실패 메시지를 별도 토픽으로 발행
            producerService.sendKafkaMessage("message-topic-dlq", message);
            ack.acknowledge(); //수동 커밋
        }
    }

    @KafkaListener(
            topics = "message-topic-dlq",
            groupId = "${spring.kafka.consumer.admin-topic-dlq-group-id}",
            containerFactory = "kafkaListener"
    )
    public void consumeMessageTopicDlq(String message) {
        System.out.println("message-topic-dlq 메시지 수신 : " + message);
    }
}
