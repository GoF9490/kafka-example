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

    // 동일한 그룹의 리스너를 2개 이상 생성 시, 카프카가 생성된 리스너들에게 파티션을 나눠줌.
    // 즉, 같은 그룹의 리스너의 코드 내용이 다르다면, 같은 토픽의 메세지도 어디 파티션에 들어가냐에 따라 다르게 동작할 수 있음.
    // 의도된 사항이 아니라면, concurrency옵션으로 리스너를 늘리거나, 그룹ID를 다르게 쓰거나.
    /*
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
     */

    @KafkaListener(
            topics = "message-topic",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListener"
            // concurrency = "2" // 동일한 로직을 수행하는 컨슈머 스레드 2개 생성
    )
    public void consumeMessageTopic(
            @Header(value = KafkaHeaders.RECEIVED_KEY, required = false) String key,
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
    public void consumeMessageTopicDlq(String message, Acknowledgment ack) {
        System.out.println("message-topic-dlq 메시지 수신 : " + message);
        ack.acknowledge();
    }

    @KafkaListener(
            topics = "message-count-5m",
            groupId = "${spring.kafka.consumer.admin-topic-dlq-group-id}",
            containerFactory = "kafkaListener"
    )
    public void consumeMessageCount5m(String message, Acknowledgment ack) {
        System.out.println("message-count-5m 메시지 수신 : " + message);
        ack.acknowledge();
    }

    @KafkaListener(
            topics = "duplicate-alerts",
            groupId = "${spring.kafka.consumer.admin-topic-dlq-group-id}",
            containerFactory = "kafkaListener"
    )
    public void consumeDuplicateAlerts(String message, Acknowledgment ack) {
        System.out.println("duplicate-alerts 메시지 수신 : " + message);
        ack.acknowledge();
    }
}
