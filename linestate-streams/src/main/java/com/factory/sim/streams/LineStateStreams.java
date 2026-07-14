package com.factory.sim.streams;

import com.factory.sim.common.JsonFields;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.HTTPServer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;

import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Properties;

/**
 * {@code factory.modbus}(적외선 실측온도)와 {@code factory.roomenv}(실내온습도)
 * 는 소스(Modbus/MQTT)도, 갱신 주기도, 스키마도 다르다. 이 앱은 그 둘을 합쳐서 하나의
 * 통일된 스키마로 {@code factory.linestate}에 다시 발행한다.
 *
 * <p>{@code factory.modbus}는 라인마다 값이 다른 스트림(KStream)이지만, {@code factory.roomenv}는
 * 실내온습도 센서가 건물 전체에 하나뿐이라 라인ID가 없는 "공장 전체 최신값 하나"짜리 테이블이다.
 * 그래서 key가 같은 레코드끼리 붙이는 보통의 스트림-테이블 join이 아니라, 모든 라인의 modbus
 * 이벤트가 항상 같은 고정 key({@link #ROOM_ENV_KEY}, {@code KafkaBridge}가 발행할 때 쓰는 것과
 * 동일한 값)를 바라보고 join하는 GlobalKTable 브로드캐스트 조인을 쓴다 (roomenv가 아직 한 번도
 * 안 왔으면 null로 처리).</p>
 *
 * <p>이 앱은 저장(DB)도, 화면 전송(WebSocket)도 하지 않는다. 정규화된 결과를 다시 Kafka에
 * 놓는 것까지만 담당하고, 그 뒤는 별도 컨슈머(DB sink / {@link com.factory.sim.ws.LiveWebSocketServer})가
 * 각자 알아서 구독한다.</p>
 *
 * <p>실행: {@code ./gradlew runLineStateStreams}</p>
 */
public final class LineStateStreams {

    // 스트림 스레드가 죽을 때(브로커 접속 끊김 등) 앱 전체가 조용히 멈추는 대신, 대시보드에서
    // "정규화 단계가 다운/느려짐/정상" 중 어디인지 구분할 수 있도록 상태를 숫자로 노출한다.
    private static final Gauge STREAMS_STATE = Gauge.build()
            .name("streams_state")
            .help("LineStateStreams 앱 상태 (2=RUNNING, 1=REBALANCING/시작 중, 0=그 외/다운)")
            .register();

    private static final Counter UNCAUGHT_EXCEPTIONS_TOTAL = Counter.build()
            .name("streams_uncaught_exceptions_total")
            .help("스트림 스레드에서 잡히지 않은 예외 발생 횟수 (스레드 교체로 이어짐)")
            .register();

    /**
     * 실내온습도는 라인별 값이 아니라 공장 전체 값 하나뿐이라서, {@code KafkaBridge}가
     * {@code factory.roomenv}에 발행할 때도 이 고정 key 하나만 쓴다. 값을 바꾸려면 그쪽도
     * 같이 맞춰야 한다.
     */
    private static final String ROOM_ENV_KEY = "factory";

    public static void main(String[] args) throws Exception {
        System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out), true, StandardCharsets.UTF_8));
        System.setErr(new PrintStream(new FileOutputStream(FileDescriptor.err), true, StandardCharsets.UTF_8));

        String bootstrapServers = System.getProperty("kafka.bootstrapServers", "localhost:9094");
        int metricsPort = Integer.parseInt(System.getProperty("metrics.port", "9102"));

        HTTPServer metricsServer = new HTTPServer.Builder().withPort(metricsPort).build();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "factory-linestate-streams");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        Topology topology = buildTopology();
        KafkaStreams streams = new KafkaStreams(topology, props);

        // 브로커 일시 단절 같은 재시도 가능한 예외로 스트림 스레드가 죽어도 프로세스 전체가
        // 내려가지 않도록, 스레드를 교체해서 계속 재시도하게 한다. 대신 발생 횟수는 카운터로
        // 남겨서 "정규화가 계속 예외를 먹으며 겨우 버티는 중"인지 알 수 있게 한다.
        streams.setUncaughtExceptionHandler(throwable -> {
            UNCAUGHT_EXCEPTIONS_TOTAL.inc();
            System.err.println("[LineStateStreams] 처리되지 않은 예외 발생, 스레드를 교체합니다: " + throwable.getMessage());
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
        });

        streams.setStateListener((newState, oldState) -> {
            STREAMS_STATE.set(stateCode(newState));
            System.out.println("[LineStateStreams] 상태 전이: " + oldState + " -> " + newState);
        });

        System.out.println("=== LineStateStreams 기동 완료 ===");
        System.out.println("Kafka bootstrap servers : " + bootstrapServers);
        System.out.println("factory.modbus + factory.roomenv -> factory.linestate (공유 온습도 broadcast join/정규화)");
        System.out.println("Prometheus 메트릭       : http://localhost:" + metricsPort + "/metrics");
        System.out.println("종료하려면 Ctrl+C 를 누르세요.");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("LineStateStreams 종료 중...");
            streams.close();
            metricsServer.stop();
        }));

        streams.start();
    }

    /** 대시보드에서 다루기 쉽게 KafkaStreams.State를 3단계 숫자로 단순화한다. */
    private static double stateCode(KafkaStreams.State newState) {
        switch (newState) {
            case RUNNING:
                return 2;
            case REBALANCING:
            case CREATED:
                return 1;
            default:
                return 0;
        }
    }

    static Topology buildTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> modbus = builder.stream("factory.modbus");
        GlobalKTable<String, String> roomEnv = builder.globalTable("factory.roomenv");

        // 라인ID로 매칭하는 게 아니라, modbus 이벤트가 어느 라인 것이든 항상 같은 고정
        // key(ROOM_ENV_KEY)로 GlobalKTable을 찾아본다 - "공장 전체가 센서 하나를 공유"를
        // 그대로 옮긴 것.
        KStream<String, String> lineState = modbus.leftJoin(
                roomEnv,
                (lineId, modbusJson) -> ROOM_ENV_KEY,
                LineStateStreams::merge);
        lineState.to("factory.linestate");

        return builder.build();
    }

    /** modbus JSON(적외선 실측온도)과 roomenv JSON(실내온습도)을 하나의 스키마로 합친다. */
    private static String merge(String modbusJson, String roomEnvJson) {
        String lineId = JsonFields.str(modbusJson, "lineId");
        double irTemp = JsonFields.num(modbusJson, "irTemp", 0);
        long ts = (long) JsonFields.num(modbusJson, "ts", System.currentTimeMillis());

        double roomTemp = JsonFields.num(roomEnvJson, "temp", 0);
        double roomHumidity = JsonFields.num(roomEnvJson, "humidity", 0);

        return String.format(
                Locale.US,
                "{\"lineId\":\"%s\",\"irTemp\":%.1f,"
                        + "\"roomTemp\":%.1f,\"roomHumidity\":%.1f,\"ts\":%d}",
                lineId, irTemp, roomTemp, roomHumidity, ts);
    }
}
