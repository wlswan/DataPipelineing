package com.factory.sim.mqtt;

import io.moquette.broker.Server;
import io.moquette.broker.config.IConfig;
import io.moquette.broker.config.MemoryConfig;

import java.util.Properties;

/**
 * 임베디드 MQTT 브로커 (Moquette).
 *
 * <p>보통 MQTT를 쓰려면 mosquitto 같은 브로커 프로그램을 컴퓨터에 별도로 설치하고
 * 실행해둬야 한다. 이 클래스는 Moquette 라이브러리를 이용해서 그 브로커 자체를
 * 이 자바 프로세스 "안에서" 함께 띄운다. 그래서 외부 프로그램 설치 없이
 * {@code ./gradlew run} 한 번으로 MQTT 브로커 + 퍼블리셔가 전부 동작한다.</p>
 *
 * <p>{@link RoomEnvPublisher}(Paho 클라이언트)는 이 브로커에 {@code tcp://localhost:포트}로
 * 접속해서 메시지를 발행(publish)한다. 즉 이 프로세스 안에서 "브로커 역할"과 "클라이언트 역할"을
 * 둘 다 하는 셈이다 - 실제 현장이라면 클라이언트만 있고 브로커는 별도 서버에 있는 게 보통이다.</p>
 */
public final class EmbeddedBroker {

    private final int port;
    private final Server mqttBroker = new Server();

    public EmbeddedBroker(int port) {
        this.port = port;
    }

    /** MQTT 브로커를 시작한다. */
    public void start() throws Exception {
        Properties props = new Properties();
        props.setProperty(IConfig.PORT_PROPERTY_NAME, String.valueOf(port));
        props.setProperty(IConfig.HOST_PROPERTY_NAME, "0.0.0.0");
        // 데모/테스트 목적이므로 사용자 인증 없이 누구나 접속 가능하게 둔다.
        props.setProperty(IConfig.ALLOW_ANONYMOUS_PROPERTY_NAME, "true");
        // 디스크에 파일을 남기지 않고 "메모리에서만" 동작하도록 영속화(persistence)를 끈다.
        props.setProperty(IConfig.PERSISTENCE_ENABLED_PROPERTY_NAME, "false");
        // 사용 통계를 외부로 보내려는 텔레메트리 기능도 꺼서, 콘솔에 불필요한 에러 로그가 안 남게 한다.
        props.setProperty(IConfig.ENABLE_TELEMETRY_NAME, "false");

        IConfig config = new MemoryConfig(props);
        mqttBroker.startServer(config);

        System.out.println("[MQTT] 임베디드 브로커 시작 완료 (port=" + port + ")");
    }

    /** MQTT 브로커를 종료한다. */
    public void stop() {
        mqttBroker.stopServer();
    }
}
