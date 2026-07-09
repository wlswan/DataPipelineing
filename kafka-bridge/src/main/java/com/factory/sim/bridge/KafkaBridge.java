package com.factory.sim.bridge;

import com.ghgande.j2mod.modbus.facade.ModbusTCPMaster;
import com.ghgande.j2mod.modbus.procimg.InputRegister;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.HTTPServer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.IMqttMessageListener;
import org.eclipse.paho.client.mqttv3.MqttCallbackExtended;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 시뮬레이터({@code com.factory.sim.Main})가 이미 떠 있는 상태에서 별도 프로세스로 실행하는
 * 브릿지. NiFi/Node-RED 같은 외부 ETL 툴 없이, 이 클래스가 직접 Modbus를 폴링하고 MQTT를
 * 구독해서 Kafka로 발행한다.
 *
 * <p>Modbus 경로: 라인마다 {@link ModbusTCPMaster}로 접속해서 1초 간격으로 Input Register
 * 0~2(화력/몰드 실측온도, 생산개수)를 읽어 {@code factory.modbus} 토픽에 key=lineId로
 * 발행한다.</p>
 *
 * <p>MQTT 경로: 임베디드 브로커에 {@code factory/+/roomEnv} 와일드카드로 구독해서, 토픽
 * 문자열에서 lineId만 뽑아 key로 쓰고 페이로드는 파싱하지 않은 채 그대로
 * {@code factory.roomenv} 토픽에 발행한다.</p>
 *
 * <p>실행: {@code ./gradlew runKafkaBridge}</p>
 */
public final class KafkaBridge {

    private static final long POLL_PERIOD_SECONDS = 1;

    // ---- 수집 단계(Modbus/MQTT -> Kafka) 상태를 그대로 Prometheus로 노출한다. ----
    // 대시보드에서 "센서에서 데이터가 안 온다"(Modbus 실패)와 "데이터는 오는데 Kafka 전송이
    // 실패한다"(브로커 다운 등)를 서로 다른 신호로 구분하기 위해 라벨을 분리해뒀다.
    private static final Counter MODBUS_POLL_TOTAL = Counter.build()
            .name("bridge_modbus_poll_total")
            .help("Modbus 폴링 시도 횟수 (라인별, result=success|failure)")
            .labelNames("line", "result")
            .register();

    private static final Gauge MODBUS_CONNECTED = Gauge.build()
            .name("bridge_modbus_connected")
            .help("라인별 Modbus TCP 접속 상태 (1=접속됨, 0=끊김)")
            .labelNames("line")
            .register();

    private static final Counter KAFKA_SEND_TOTAL = Counter.build()
            .name("bridge_kafka_send_total")
            .help("Kafka 발행 시도 횟수 (토픽별, result=success|failure)")
            .labelNames("topic", "result")
            .register();

    private static final Gauge MQTT_CONNECTED = Gauge.build()
            .name("bridge_mqtt_connected")
            .help("MQTT 브로커 접속 상태 (1=접속됨, 0=끊김)")
            .register();

    private static final Counter MQTT_RECONNECT_TOTAL = Counter.build()
            .name("bridge_mqtt_reconnect_total")
            .help("MQTT 자동 재접속 성공 횟수")
            .register();

    public static void main(String[] args) throws Exception {
        // 윈도우 콘솔에서 한글이 깨지는 것을 막기 위해 System.out/err을 UTF-8로 강제한다.
        System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out), true, StandardCharsets.UTF_8));
        System.setErr(new PrintStream(new FileOutputStream(FileDescriptor.err), true, StandardCharsets.UTF_8));

        int lineCount = Integer.parseInt(System.getProperty("lines", "7"));
        int modbusBasePort = Integer.parseInt(System.getProperty("modbus.basePort", "502"));
        int mqttPort = Integer.parseInt(System.getProperty("mqtt.port", "1883"));
        String bootstrapServers = System.getProperty("kafka.bootstrapServers", "localhost:9094");
        int metricsPort = Integer.parseInt(System.getProperty("metrics.port", "9101"));

        HTTPServer metricsServer = new HTTPServer.Builder().withPort(metricsPort).build();

        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.ACKS_CONFIG, "1");
        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);

        // ---- Modbus 경로: 라인마다 폴러를 하나씩 만들어 1초 간격으로 실행 ----
        ScheduledExecutorService executor = new ScheduledThreadPoolExecutor(lineCount, runnable -> {
            Thread thread = new Thread(runnable, "kafka-bridge-modbus-poller");
            thread.setDaemon(true);
            return thread;
        });

        List<ModbusPoller> pollers = new ArrayList<>(lineCount);
        for (int lineNumber = 1; lineNumber <= lineCount; lineNumber++) {
            int port = modbusBasePort + (lineNumber - 1);
            ModbusPoller poller = new ModbusPoller(lineNumber, port, producer);
            pollers.add(poller);
            executor.scheduleAtFixedRate(poller, 0, POLL_PERIOD_SECONDS, TimeUnit.SECONDS);
        }

        // ---- MQTT 경로: 브로커 하나에 와일드카드로 구독, lineId만 뽑아 key로 재발행 ----
        String brokerUrl = "tcp://localhost:" + mqttPort;
        MqttClient mqttClient = new MqttClient(brokerUrl, "factory-sim-kafka-bridge", new MemoryPersistence());

        // connectComplete/connectionLost는 메시지 수신과 별개의 "연결 자체" 생명주기 이벤트라서,
        // 구독 시 넘기는 IMqttMessageListener와는 별도로 setCallback()에 등록해야 잡을 수 있다.
        // connect() 호출 전에 등록해야 최초 접속도 connectComplete로 잡힌다.
        mqttClient.setCallback(new MqttCallbackExtended() {
            @Override
            public void connectComplete(boolean reconnect, String serverURI) {
                MQTT_CONNECTED.set(1);
                if (reconnect) {
                    MQTT_RECONNECT_TOTAL.inc();
                    System.out.println("[KafkaBridge] MQTT 재접속 성공: " + serverURI);
                }
            }

            @Override
            public void connectionLost(Throwable cause) {
                MQTT_CONNECTED.set(0);
                System.err.println("[KafkaBridge] MQTT 연결 끊김 (자동 재접속 대기 중): " + cause.getMessage());
            }

            @Override
            public void messageArrived(String topic, MqttMessage message) {
                // 구독은 아래 IMqttMessageListener(roomEnvListener)가 처리하므로 여기서는 사용하지 않는다.
            }

            @Override
            public void deliveryComplete(IMqttDeliveryToken token) {
            }
        });

        MqttConnectOptions options = new MqttConnectOptions();
        options.setAutomaticReconnect(true);
        options.setCleanSession(true);
        options.setConnectionTimeout(10);
        mqttClient.connect(options);

        IMqttMessageListener roomEnvListener = (topic, message) -> {
            // 토픽 형태: factory/line{n}/roomEnv -> parts[1] = "line{n}"
            String[] parts = topic.split("/");
            String lineId = parts.length >= 2 ? parts[1] : "unknown";
            String payload = new String(message.getPayload(), StandardCharsets.UTF_8);
            producer.send(new ProducerRecord<>("factory.roomenv", lineId, payload), (metadata, exception) -> {
                if (exception != null) {
                    KAFKA_SEND_TOTAL.labels("factory.roomenv", "failure").inc();
                    System.err.println("[KafkaBridge] " + lineId + " roomEnv Kafka 발행 실패: " + exception.getMessage());
                } else {
                    KAFKA_SEND_TOTAL.labels("factory.roomenv", "success").inc();
                }
            });
        };
        mqttClient.subscribe("factory/+/roomEnv", 0, roomEnvListener);

        System.out.println("=== KafkaBridge 기동 완료 (" + lineCount + "개 라인) ===");
        System.out.println("Kafka bootstrap servers : " + bootstrapServers);
        System.out.println("Modbus 폴링 대상        : localhost:" + modbusBasePort + " ~ " + (modbusBasePort + lineCount - 1) + " (1초 간격) -> topic factory.modbus");
        System.out.println("MQTT 구독               : " + brokerUrl + " factory/+/roomEnv -> topic factory.roomenv");
        System.out.println("Prometheus 메트릭       : http://localhost:" + metricsPort + "/metrics");
        System.out.println("종료하려면 Ctrl+C 를 누르세요.");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("KafkaBridge 종료 중...");
            executor.shutdownNow();
            for (ModbusPoller poller : pollers) {
                poller.close();
            }
            try {
                mqttClient.disconnect();
            } catch (Exception ignored) {
                // 종료 과정에서 발생하는 예외는 프로세스 종료를 막을 만큼 중요하지 않으므로 무시한다.
            }
            producer.flush();
            producer.close();
            metricsServer.stop();
        }));

        Thread.currentThread().join();
    }

    /** 라인 하나의 Modbus TCP 접속을 유지하면서 1초마다 Input Register를 읽어 Kafka로 발행한다. */
    private static final class ModbusPoller implements Runnable {
        private final int lineNumber;
        private final String lineId;
        private final String host = "localhost";
        private final int port;
        private final KafkaProducer<String, String> producer;
        private final ModbusTCPMaster master;
        private boolean connected = false;

        ModbusPoller(int lineNumber, int port, KafkaProducer<String, String> producer) {
            this.lineNumber = lineNumber;
            this.lineId = "line" + lineNumber;
            this.port = port;
            this.producer = producer;
            this.master = new ModbusTCPMaster(host, port);
        }

        @Override
        public void run() {
            try {
                if (!connected) {
                    master.connect();
                    connected = true;
                    MODBUS_CONNECTED.labels(lineId).set(1);
                    System.out.println("[KafkaBridge] Line " + lineNumber + " Modbus 접속 성공 (" + host + ":" + port + ")");
                }

                InputRegister[] regs = master.readInputRegisters(0, 3);
                double fireActual = regs[0].getValue() / 10.0;
                double moldActual = regs[1].getValue() / 10.0;
                int servedCount = regs[2].getValue();
                MODBUS_POLL_TOTAL.labels(lineId, "success").inc();

                String json = String.format(
                        Locale.US,
                        "{\"lineId\":\"%s\",\"fireActual\":%.1f,\"moldActual\":%.1f,\"servedCount\":%d,\"ts\":%d}",
                        lineId, fireActual, moldActual, servedCount, System.currentTimeMillis());

                producer.send(new ProducerRecord<>("factory.modbus", lineId, json), (metadata, exception) -> {
                    if (exception != null) {
                        KAFKA_SEND_TOTAL.labels("factory.modbus", "failure").inc();
                        System.err.println("[KafkaBridge] Line " + lineNumber + " Kafka 발행 실패: " + exception.getMessage());
                    } else {
                        KAFKA_SEND_TOTAL.labels("factory.modbus", "success").inc();
                    }
                });
            } catch (Exception e) {
                // 접속이 끊겼을 수 있으니 다음 tick에 재접속을 시도하도록 상태를 되돌린다.
                connected = false;
                MODBUS_CONNECTED.labels(lineId).set(0);
                MODBUS_POLL_TOTAL.labels(lineId, "failure").inc();
                System.err.println("[KafkaBridge] Line " + lineNumber + " Modbus 폴링 실패: " + e.getMessage());
            }
        }

        void close() {
            if (connected) {
                master.disconnect();
            }
        }
    }
}
