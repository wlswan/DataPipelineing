package com.factory.sim.mqtt;

import com.factory.sim.RoomEnvironment;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * MQTT 퍼블리셔 (Paho 클라이언트).
 *
 * <p>{@link RoomEnvironment}에 있는 "실내온습도"만 담당한다. 문제 설정상 실내온습도는
 * 화력/몰드온도 같은 구형 PLC 직결 센서와 달리 "최신 IoT형 센서"라고 가정했기 때문에,
 * Modbus가 아니라 MQTT로 값을 흘려보낸다.</p>
 *
 * <p>실내온습도 센서는 라인마다 있는 게 아니라 건물 전체에 하나만 있으므로, 이 퍼블리셔도
 * 라인 개수와 무관하게 프로세스 전체에서 단 하나만 떠서 {@code factory/roomEnv} 토픽 하나로
 * 2초마다 JSON 문자열 {@code {"temp":22.3,"humidity":51.2,"ts":1719900000000}} 형태의
 * 메시지를 발행한다.</p>
 *
 * <p>이 클래스도 {@link RoomEnvironment}를 읽기만 한다. 값을 계산하거나 바꾸는 일은 절대
 * 하지 않는다.</p>
 */
public final class RoomEnvPublisher {

    private static final long PUBLISH_PERIOD_SECONDS = 2;
    private static final String TOPIC = "factory/roomEnv";

    private final String brokerUrl;
    private final RoomEnvironment environment;

    /** 2초마다 발행 작업을 반복 실행할 전용 스레드. */
    private final ScheduledExecutorService executor;

    private MqttClient client;

    public RoomEnvPublisher(String brokerUrl, RoomEnvironment environment) {
        this.brokerUrl = brokerUrl;
        this.environment = environment;
        this.executor = new ScheduledThreadPoolExecutor(1, runnable -> {
            Thread thread = new Thread(runnable, "mqtt-publisher-roomenv");
            thread.setDaemon(true);
            return thread;
        });
    }

    /** 브로커에 접속하고, 2초 주기 발행을 시작한다. */
    public void start() throws Exception {
        String clientId = "factory-sim-publisher-roomenv-" + System.currentTimeMillis();
        // MemoryPersistence: 재전송을 위해 메시지를 디스크에 남기지 않고 메모리에만 잠깐 보관한다.
        client = new MqttClient(brokerUrl, clientId, new MemoryPersistence());

        MqttConnectOptions options = new MqttConnectOptions();
        options.setAutomaticReconnect(true);
        options.setCleanSession(true);
        options.setConnectionTimeout(10);

        client.connect(options);
        System.out.println("[MQTT] RoomEnv Publisher 접속 완료 (broker=" + brokerUrl + ", topic=" + TOPIC + ")");

        executor.scheduleAtFixedRate(this::publishOnce, 0, PUBLISH_PERIOD_SECONDS, TimeUnit.SECONDS);
    }

    /** 현재 실내온습도를 JSON으로 만들어 한 번 발행한다. */
    private void publishOnce() {
        try {
            RoomEnvironment.RoomEnv env = environment.get();

            // 외부 JSON 라이브러리 없이, 필드가 3개뿐인 단순한 구조라 문자열로 직접 조립했다.
            String json = String.format(
                    Locale.US,
                    "{\"temp\":%.1f,\"humidity\":%.1f,\"ts\":%d}",
                    env.temp, env.humidity, System.currentTimeMillis());

            MqttMessage message = new MqttMessage(json.getBytes(StandardCharsets.UTF_8));
            message.setQos(0);
            client.publish(TOPIC, message);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /** 발행을 멈추고 브로커 접속을 끊는다. */
    public void stop() {
        executor.shutdownNow();
        try {
            if (client != null && client.isConnected()) {
                client.disconnect();
            }
        } catch (Exception ignored) {
            // 종료 과정에서 발생하는 예외는 프로세스 종료를 막을 만큼 중요하지 않으므로 무시한다.
        }
    }
}
