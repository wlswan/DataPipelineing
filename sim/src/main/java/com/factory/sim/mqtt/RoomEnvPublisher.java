package com.factory.sim.mqtt;

import com.factory.sim.FactoryState;
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
 * <p>{@link FactoryState}에 있는 값 중 "실내온습도"만 담당한다. 문제 설정상 실내온습도는
 * 화력/몰드온도 같은 구형 PLC 직결 센서와 달리 "최신 IoT형 센서"라고 가정했기 때문에,
 * Modbus가 아니라 MQTT로 값을 흘려보낸다.</p>
 *
 * <p>2초마다 {@code factory/line{n}/roomEnv} 토픽으로 JSON 문자열
 * {@code {"temp":22.3,"humidity":51.2,"ts":1719900000000}} 형태의 메시지를 발행한다. 여러
 * 라인이 같은 임베디드 브로커 하나를 공유하기 때문에, 라인마다 토픽과 클라이언트 ID를
 * 다르게 줘서 서로 섞이거나 접속이 충돌하지 않게 한다.</p>
 *
 * <p>이 클래스도 FactoryState를 읽기만 한다 ({@link FactoryState#getRoomEnv()}). 값을 계산하거나
 * 바꾸는 일은 절대 하지 않는다.</p>
 */
public final class RoomEnvPublisher {

    private static final long PUBLISH_PERIOD_SECONDS = 2;

    private final int lineNumber;
    private final String topic;
    private final String brokerUrl;
    private final FactoryState state;

    /** 2초마다 발행 작업을 반복 실행할 전용 스레드. */
    private final ScheduledExecutorService executor;

    private MqttClient client;

    public RoomEnvPublisher(String brokerUrl, FactoryState state, int lineNumber) {
        this.brokerUrl = brokerUrl;
        this.state = state;
        this.lineNumber = lineNumber;
        this.topic = "factory/line" + lineNumber + "/roomEnv";
        this.executor = new ScheduledThreadPoolExecutor(1, runnable -> {
            Thread thread = new Thread(runnable, "mqtt-publisher-line" + lineNumber);
            thread.setDaemon(true);
            return thread;
        });
    }

    /** 브로커에 접속하고, 2초 주기 발행을 시작한다. */
    public void start() throws Exception {
        String clientId = "factory-sim-publisher-line" + lineNumber + "-" + System.currentTimeMillis();
        // MemoryPersistence: 재전송을 위해 메시지를 디스크에 남기지 않고 메모리에만 잠깐 보관한다.
        client = new MqttClient(brokerUrl, clientId, new MemoryPersistence());

        MqttConnectOptions options = new MqttConnectOptions();
        options.setAutomaticReconnect(true);
        options.setCleanSession(true);
        options.setConnectionTimeout(10);

        client.connect(options);
        System.out.println("[MQTT] Line " + lineNumber + " Publisher 접속 완료 (broker=" + brokerUrl + ", topic=" + topic + ")");

        executor.scheduleAtFixedRate(this::publishOnce, 0, PUBLISH_PERIOD_SECONDS, TimeUnit.SECONDS);
    }

    /** 현재 실내온습도를 JSON으로 만들어 한 번 발행한다. */
    private void publishOnce() {
        try {
            FactoryState.RoomEnv env = state.getRoomEnv();

            // 외부 JSON 라이브러리 없이, 필드가 3개뿐인 단순한 구조라 문자열로 직접 조립했다.
            String json = String.format(
                    Locale.US,
                    "{\"temp\":%.1f,\"humidity\":%.1f,\"ts\":%d}",
                    env.temp, env.humidity, System.currentTimeMillis());

            MqttMessage message = new MqttMessage(json.getBytes(StandardCharsets.UTF_8));
            message.setQos(0);
            client.publish(topic, message);
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
