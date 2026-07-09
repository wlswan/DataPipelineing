package com.factory.sim.client;

import org.eclipse.paho.client.mqttv3.IMqttMessageListener;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * MQTT 테스트 클라이언트.
 *
 * <p>시뮬레이터({@code com.factory.sim.Main})가 먼저 떠 있어야 한다. 시뮬레이터 프로세스 안에서
 * 실행 중인 임베디드 브로커(Moquette)에 Paho 클라이언트로 접속해서 {@code factory/+/roomEnv}
 * 와일드카드 토픽을 구독(subscribe)하고, 모든 라인의 실내온습도 메시지가 실제로 도착하는지
 * 확인한다 (수신 로그에 실제 토픽명이 찍히므로 어느 라인에서 온 메시지인지 구분된다).</p>
 *
 * <p>발행 주기가 2초이므로, 메시지 3개를 받을 때까지 최대 30초 기다린다.</p>
 *
 * <p>실행: {@code ./gradlew runMqttTestClient}</p>
 */
public final class MqttTestClient {

    private static final String TOPIC_FILTER = "factory/+/roomEnv";
    private static final int MESSAGES_TO_WAIT_FOR = 3;
    private static final int WAIT_TIMEOUT_SECONDS = 30;

    public static void main(String[] args) throws Exception {
        // 윈도우 콘솔에서 한글이 깨지는 것을 막기 위해 System.out을 UTF-8로 강제한다.
        System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out), true, StandardCharsets.UTF_8));

        int port = args.length > 0
                ? Integer.parseInt(args[0])
                : Integer.parseInt(System.getProperty("mqtt.port", "1883"));
        String brokerUrl = "tcp://localhost:" + port;

        MqttClient client = new MqttClient(brokerUrl, "factory-sim-test-subscriber", new MemoryPersistence());
        MqttConnectOptions options = new MqttConnectOptions();
        options.setCleanSession(true);
        client.connect(options);
        System.out.println("[MQTT 테스트] 접속 성공: " + brokerUrl);

        // 메시지가 MESSAGES_TO_WAIT_FOR개 도착할 때까지 메인 스레드를 대기시키기 위한 장치.
        // subscribe()의 콜백은 별도의 MQTT 클라이언트 내부 스레드에서 호출되기 때문에
        // CountDownLatch로 "몇 개 받았는지"를 스레드 안전하게 센다.
        CountDownLatch latch = new CountDownLatch(MESSAGES_TO_WAIT_FOR);

        IMqttMessageListener listener = (topic, message) -> {
            String payload = new String(message.getPayload(), StandardCharsets.UTF_8);
            System.out.println("  [수신] topic=" + topic + " payload=" + payload);
            latch.countDown();
        };

        client.subscribe(TOPIC_FILTER, 0, listener);
        System.out.println("토픽 구독 시작: " + TOPIC_FILTER);
        System.out.println("메시지 " + MESSAGES_TO_WAIT_FOR + "개 도착을 최대 " + WAIT_TIMEOUT_SECONDS + "초간 대기합니다...");

        boolean received = latch.await(WAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        System.out.println();
        if (received) {
            System.out.println("테스트 성공: 실내온습도 메시지 " + MESSAGES_TO_WAIT_FOR + "개를 정상적으로 수신했습니다.");
        } else {
            System.out.println("테스트 실패: 제한 시간 내에 메시지를 충분히 받지 못했습니다. 시뮬레이터가 떠 있는지 확인하세요.");
        }

        client.disconnect();
        System.exit(received ? 0 : 1);
    }
}
