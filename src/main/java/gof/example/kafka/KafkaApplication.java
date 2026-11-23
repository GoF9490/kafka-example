package gof.example.kafka;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.SlidingWindows;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.time.Duration;
import java.util.Properties;

@SpringBootApplication
public class KafkaApplication {

	public static void main(String[] args) {
		SpringApplication.run(KafkaApplication.class, args);

		/*
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "duplicate-detector-app");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		// 빠른 실시간 감지를 위해 상태 저장소 캐시 비활성화 (즉시 다운스트림 전송)
		props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);

		StreamsBuilder builder = new StreamsBuilder();
		KStream<String, String> source = builder.stream("raw-messages");

		source
				// 1. 메시지 본문(Value)을 기준으로 그룹화 (기존 Key가 이미 메시지 ID라면 groupByKey() 사용)
				.groupBy((key, value) -> value)
				// 2. 3초 크기의 슬라이딩 윈도우 설정 (유예 시간 0초)
				.windowedBy(SlidingWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(3)))
				// 3. 윈도우 내 개수 집계 (내부 RocksDB에 자동 저장·관리)
				.count()
				// 4. 집계 결과(KTable)를 다시 스트림으로 변환하여 5회 이상인 경우만 필터링
				.toStream()
				.filter((windowedKey, count) -> count != null && count >= 5)
				// 5. 알림 메시지 포맷팅 후 출력 토픽으로 발행
				.mapValues((windowedKey, count) ->
						String.format("[중복 감지] 메시지='%s', 3초 내 발생횟수=%d회", windowedKey.key(), count)
				)
				.selectKey((windowedKey, alertMsg) -> windowedKey.key())
				.to("duplicate-alerts");

		KafkaStreams streams = new KafkaStreams(builder.build(), props);
		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
		streams.start();
		 */
	}
}
