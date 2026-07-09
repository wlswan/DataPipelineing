package com.factory.sim.ws;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;

import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * {@code factory.linestate} 토픽(Kafka Streams가 정규화한 라인별 통합 상태)을 구독해서,
 * 도착하는 메시지를 그대로 연결된 모든 브라우저 클라이언트에게 relay하는 "핫 패스" 백엔드.
 *
 * <p>DB를 거치지 않는다 — 목적이 실시간 화면 반영이기 때문에, 지연을 늘리는 저장 단계를
 * 건너뛰고 Kafka에서 받은 값을 곧바로 WebSocket으로 내보낸다. 이력 저장(TimescaleDB 등)은
 * 같은 {@code factory.linestate} 토픽을 구독하는 별도 컨슈머 그룹이 독립적으로 담당하는
 * 구조라서, 이 클래스는 그 부분과 아예 무관하다.</p>
 *
 * <p>실행: {@code ./gradlew runLiveWebSocketServer}</p>
 */
public final class LiveWebSocketServer extends WebSocketServer {

    public LiveWebSocketServer(int port) {
        super(new InetSocketAddress(port));
    }

    @Override
    public void onOpen(WebSocket conn, ClientHandshake handshake) {
        System.out.println("[WS] 클라이언트 접속: " + conn.getRemoteSocketAddress());
    }

    @Override
    public void onClose(WebSocket conn, int code, String reason, boolean remote) {
        System.out.println("[WS] 클라이언트 종료: " + conn.getRemoteSocketAddress());
    }

    @Override
    public void onMessage(WebSocket conn, String message) {
        // 브라우저 -> 서버 방향 메시지는 쓰지 않는다 (읽기 전용 relay).
    }

    @Override
    public void onError(WebSocket conn, Exception ex) {
        System.err.println("[WS] 에러: " + ex.getMessage());
    }

    @Override
    public void onStart() {
        System.out.println("[WS] 서버 시작 완료 (port=" + getPort() + ")");
    }

    public static void main(String[] args) {
        System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out), true, StandardCharsets.UTF_8));
        System.setErr(new PrintStream(new FileOutputStream(FileDescriptor.err), true, StandardCharsets.UTF_8));

        int wsPort = Integer.parseInt(System.getProperty("ws.port", "8081"));
        String bootstrapServers = System.getProperty("kafka.bootstrapServers", "localhost:9094");

        LiveWebSocketServer server = new LiveWebSocketServer(wsPort);
        server.setReuseAddr(true);
        server.start();

        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "factory-linestate-ws-backend");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singletonList("factory.linestate"));

        Thread relayThread = new Thread(() -> {
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                    for (ConsumerRecord<String, String> record : records) {
                        server.broadcast(record.value());
                    }
                }
            } catch (WakeupException ignored) {
                // 종료 신호로 poll()이 깨어난 것 - 정상 흐름
            } finally {
                consumer.close();
            }
        }, "linestate-ws-relay");
        relayThread.setDaemon(true);
        relayThread.start();

        System.out.println("=== LiveWebSocketServer 기동 완료 ===");
        System.out.println("Kafka bootstrap servers : " + bootstrapServers);
        System.out.println("WebSocket 포트          : " + wsPort + " (factory.linestate -> 브라우저로 relay)");
        System.out.println("종료하려면 Ctrl+C 를 누르세요.");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("LiveWebSocketServer 종료 중...");
            consumer.wakeup();
            try {
                server.stop();
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }));
    }
}
