package gof.example.kafka.service;

import gof.example.kafka.dtos.CategoryStats;
import gof.example.kafka.dtos.OrderEvent;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.WindowStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.support.serializer.JacksonJsonSerde;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

@Component
public class KafkaStreamsExample {

    private static final DateTimeFormatter FORMATTER =
            DateTimeFormatter.ofPattern("HH:mm:ss").withZone(ZoneId.of("Asia/Seoul"));

    // 단일 streams 구성
    /*
    @Autowired
    public void buildStatsPipeline(StreamsBuilder builder, JsonMapper jsonMapper) {
        // 1. JSON <-> 자바 객체(DTO) 변환을 담당할 Serde(직렬화/역직렬화기) 생성
        JacksonJsonSerde<OrderEvent> orderSerde = new JacksonJsonSerde<>(OrderEvent.class, jsonMapper);
        JacksonJsonSerde<CategoryStats> statsSerde = new JacksonJsonSerde<>(CategoryStats.class, jsonMapper);

        // 타입 헤더(__TypeId__)를 무시해야 할 경우
        // orderSerde.ignoreTypeHeaders();

        // 2. [Source] 'order-events' 토픽에서 데이터를 실시간으로 읽어 KStream 생성
        KStream<String, OrderEvent> orderStream = builder.stream(
                "order-events",
                Consumed.with(Serdes.String(), orderSerde) // Key는 String, Value는 OrderEvent로 읽음
        );

        orderStream
                // 3. [Key 재설정] 기존 메시지 키 대신 '주문 카테고리(예: ELECTRONICS)'를 새로운 키로 지정
                .selectKey((key, order) -> order.category())
                // 4. [그룹화] 같은 카테고리끼리 모아서 집계할 수 있도록 그룹화 (내부적으로 리파티셔닝 발생)
                .groupByKey(Grouped.with(Serdes.String(), orderSerde))
                // 5. [시간 창 설정] 1분 단위로 겹치지 않는 고정 구간(00초~59초)을 나누고 지연 데이터는 허용 안 함
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)))
                // 6. [상태 기반 집계] 카테고리별 주문 건수, 총액, 평균을 내장 RocksDB에 실시간 누적 계산
                .aggregate(
                        // 6-1. 각 1분 윈도우가 처음 시작될 때 사용할 초기 통계 객체 (0건, 0원, 평균 0.0)
                        () -> new CategoryStats(0L, 0L, 0.0),
                        // 6-2. 새 주문이 들어올 때마다 기존 통계(currentStats)에 주문 금액을 더해 새 통계 반환
                        (category, order, currentStates) -> currentStates.add(order.amount()),
                        // 6-3. 계산 중인 중간 통계 데이터를 저장할 로컬 RocksDB 저장소 이름 및 타입 지정
                        Materialized.<String, CategoryStats, WindowStore<Bytes, byte[]>>as("category-stats-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(statsSerde)
                )
                // 7. [출력 억제] 주문이 들어올 때마다 중간 결과를 내보내지 않고, 1분 윈도우가 완전히 끝날 때까지 대기
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
                // 8. [스트림 전환] 집계 테이블(KTable)에서 확정된 최종 결과만 다시 이벤트 스트림(KStream)으로 변환
                .toStream()
                // 9. [출력 포맷 가공] 사람이 읽기 쉬운 문자열 리포트 형태로 Key와 Value를 변환
                .map((windowedKey, stats) -> {
                    // 윈도우의 시작 시각과 종료 시각 추출
                    String startTime = FORMATTER.format(Instant.ofEpochMilli(windowedKey.window().start()));
                    String endTime = FORMATTER.format(Instant.ofEpochMilli(windowedKey.window().end()));
                    // 출력 메시지의 새로운 Key (예: "ELECTRONICS_11:00:00")
                    String newKey = windowedKey.key() + "_" + startTime;

                    // 출력 메시지의 Value (최종 통계 요약 문자열)
                    String summary = String.format(
                            "[%s - %s] 카테고리: %s | 주문건수 : %건 | 총매출: %,d원 | 평균단가: %,.0f원",
                            startTime, endTime, windowedKey.key(), stats.count(), stats.totalAmount(), stats.averageAmount()
                    );
                    return KeyValue.pair(newKey, summary);
                })
                // 10. [Sink] 최종 가공된 통계 문자열을 'order-stats-1m' 토픽으로 발행
                .to("order-stats-1m", Produced.with(Serdes.String(), Serdes.String()));
    }
     */

    // 다중 streams 구성
    @Autowired
    public void messageCountPipeline(@Qualifier("messageCountStreamsBuilder") StreamsBuilder builder) {
        KStream<String, String> source = builder.stream(
                "message-topic",
                Consumed.with(Serdes.String(), Serdes.String())
        );

        source
                .groupBy((key, value) -> "TOTAL", Grouped.with(Serdes.String(), Serdes.String()))
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5)))
                .count()
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
                .toStream()
                .map((windowedKey, count) -> {
                    String start = FORMATTER.format(Instant.ofEpochMilli(windowedKey.window().start()));
                    String end = FORMATTER.format(Instant.ofEpochMilli(windowedKey.window().end()));
                    String result = String.format(
                            "[%s ~ %s] 5분간 총 메시지 수: %d건",
                            start, end, count
                    );
                    return KeyValue.pair(start, result);
                })
                .to("message-count-5m", Produced.with(Serdes.String(), Serdes.String()));
    }

    @Autowired
    public void duplicateAlertsPipeline(@Qualifier("duplicateAlertStreamsBuilder") StreamsBuilder builder) {
        KStream<String, String> source = builder.stream(
                "message-topic",
                Consumed.with(Serdes.String(), Serdes.String())
        );

        source
                .groupBy((key, value) -> "TOTAL", Grouped.with(Serdes.String(), Serdes.String()))
                .windowedBy(SlidingWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(3)))
                .count()
                .toStream()
                .filter((windowedKey, count) -> count != null && count >= 5)
                .mapValues((windowedKey, count) ->
                        String.format("[중복 감지] 메세지='%s', 3초 내 발생횟수=%d회", windowedKey.key(), count)
                )
                .selectKey((windowedKey, alertMsg) -> windowedKey.key())
                .to("duplicate-alerts", Produced.with(Serdes.String(), Serdes.String()));
    }
}
