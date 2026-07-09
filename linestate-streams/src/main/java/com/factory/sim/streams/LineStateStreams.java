package com.factory.sim.streams;

import com.factory.sim.common.JsonFields;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Properties;

/**
 * {@code factory.modbus}(화력/몰드 실측온도, 생산개수)와 {@code factory.roomenv}(실내온습도)
 * 는 소스(Modbus/MQTT)도, 갱신 주기도, 스키마도 다르다. 이 앱은 그 둘을 라인ID(key) 기준으로
 * 합쳐서 하나의 통일된 스키마로 {@code factory.linestate}에 다시 발행한다.
 *
 * <p>{@code factory.modbus}는 1초마다 확정적으로 오는 스트림(KStream)으로, {@code
 * factory.roomenv}는 "그 라인의 가장 최근 온습도 값"을 유지하는 테이블(KTable)로 다룬다.
 * modbus 이벤트가 올 때마다 그 시점의 최신 roomenv 값을 붙이는 stream-table left join이다
 * (roomenv가 아직 한 번도 안 왔으면 null로 처리).</p>
 *
 * <p>이 앱은 저장(DB)도, 화면 전송(WebSocket)도 하지 않는다. 정규화된 결과를 다시 Kafka에
 * 놓는 것까지만 담당하고, 그 뒤는 별도 컨슈머(DB sink / {@link com.factory.sim.ws.LiveWebSocketServer})가
 * 각자 알아서 구독한다.</p>
 *
 * <p>실행: {@code ./gradlew runLineStateStreams}</p>
 */
public final class LineStateStreams {

    public static void main(String[] args) {
        System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out), true, StandardCharsets.UTF_8));
        System.setErr(new PrintStream(new FileOutputStream(FileDescriptor.err), true, StandardCharsets.UTF_8));

        String bootstrapServers = System.getProperty("kafka.bootstrapServers", "localhost:9094");

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "factory-linestate-streams");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        Topology topology = buildTopology();
        KafkaStreams streams = new KafkaStreams(topology, props);

        System.out.println("=== LineStateStreams 기동 완료 ===");
        System.out.println("Kafka bootstrap servers : " + bootstrapServers);
        System.out.println("factory.modbus + factory.roomenv -> factory.linestate (라인ID로 join/정규화)");
        System.out.println("종료하려면 Ctrl+C 를 누르세요.");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("LineStateStreams 종료 중...");
            streams.close();
        }));

        streams.start();
    }

    static Topology buildTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> modbus = builder.stream("factory.modbus");
        KTable<String, String> roomEnv = builder.table("factory.roomenv");

        KStream<String, String> lineState = modbus.leftJoin(roomEnv, LineStateStreams::merge);
        lineState.to("factory.linestate");

        return builder.build();
    }

    /** modbus JSON(화력/몰드온도, 생산개수)과 roomenv JSON(실내온습도)을 하나의 스키마로 합친다. */
    private static String merge(String modbusJson, String roomEnvJson) {
        String lineId = JsonFields.str(modbusJson, "lineId");
        double fireActual = JsonFields.num(modbusJson, "fireActual", 0);
        double moldActual = JsonFields.num(modbusJson, "moldActual", 0);
        int servedCount = (int) JsonFields.num(modbusJson, "servedCount", 0);
        long ts = (long) JsonFields.num(modbusJson, "ts", System.currentTimeMillis());

        double roomTemp = JsonFields.num(roomEnvJson, "temp", 0);
        double roomHumidity = JsonFields.num(roomEnvJson, "humidity", 0);

        return String.format(
                Locale.US,
                "{\"lineId\":\"%s\",\"fireActual\":%.1f,\"moldActual\":%.1f,\"servedCount\":%d,"
                        + "\"roomTemp\":%.1f,\"roomHumidity\":%.1f,\"ts\":%d}",
                lineId, fireActual, moldActual, servedCount, roomTemp, roomHumidity, ts);
    }
}
