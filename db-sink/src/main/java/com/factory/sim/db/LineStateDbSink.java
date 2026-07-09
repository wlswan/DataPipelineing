package com.factory.sim.db;

import com.factory.sim.common.JsonFields;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.HTTPServer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * {@code factory.linestate}를 독립된 컨슈머 그룹으로 구독해서 TimescaleDB에 적재하는
 * "콜드 패스" 백엔드.
 *
 * <p>{@link com.factory.sim.ws.LiveWebSocketServer}(핫 패스)와 컨슈머 그룹이 다르기 때문에,
 * 이 클래스가 DB에 쓰느라 잠깐 느려지거나 멈춰도 실시간 화면 쪽에는 전혀 영향이 없다.
 * 반대로 화면 쪽이 밀려도 이 적재 작업에는 영향이 없다.</p>
 *
 * <p>레코드 하나마다 insert/commit 하면 초당 라인 수만큼 왕복이 생겨 비효율적이므로,
 * poll() 한 번에 받은 배치를 통째로 하나의 트랜잭션으로 묶어 batch insert 한다.</p>
 *
 * <p>실행: {@code ./gradlew runLineStateDbSink}</p>
 */
public final class LineStateDbSink {

    private static final String INSERT_SQL =
            "INSERT INTO line_state "
                    + "(ts, line_id, fire_actual, mold_actual, room_temp, room_humidity, served_count) "
                    + "VALUES (?, ?, ?, ?, ?, ?, ?)";

    // ---- 저장 단계(TimescaleDB 적재) 상태를 Prometheus로 노출한다. ----
    // "컨슈머는 살아있는데 DB 적재만 실패하는 중"인 상황(예: DB 컨테이너 재시작)을
    // Kafka consumer group lag만으로는 구분하기 어려워서, 적재 성공/실패를 직접 센다.
    private static final Counter INSERT_BATCHES_TOTAL = Counter.build()
            .name("dbsink_insert_batches_total")
            .help("배치 삽입 시도 횟수 (result=success|failure)")
            .labelNames("result")
            .register();

    private static final Counter RECORDS_INSERTED_TOTAL = Counter.build()
            .name("dbsink_records_inserted_total")
            .help("TimescaleDB에 적재된 누적 레코드 수")
            .register();

    private static final Gauge DB_CONNECTED = Gauge.build()
            .name("dbsink_db_connected")
            .help("TimescaleDB 연결 상태 (1=연결됨, 0=끊김/재연결 시도 중)")
            .register();

    private static final Counter RECONNECT_TOTAL = Counter.build()
            .name("dbsink_reconnect_total")
            .help("TimescaleDB 재연결 성공 횟수")
            .register();

    public static void main(String[] args) throws Exception {
        System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out), true, StandardCharsets.UTF_8));
        System.setErr(new PrintStream(new FileOutputStream(FileDescriptor.err), true, StandardCharsets.UTF_8));

        String bootstrapServers = System.getProperty("kafka.bootstrapServers", "localhost:9094");
        String jdbcUrl = System.getProperty("db.url", "jdbc:postgresql://localhost:5432/factory");
        String dbUser = System.getProperty("db.user", "factory");
        String dbPassword = System.getProperty("db.password", "factory");
        int metricsPort = Integer.parseInt(System.getProperty("metrics.port", "9103"));

        HTTPServer metricsServer = new HTTPServer.Builder().withPort(metricsPort).build();

        DbHandle db = openConnection(jdbcUrl, dbUser, dbPassword);

        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "factory-linestate-db-sink");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singletonList("factory.linestate"));

        System.out.println("=== LineStateDbSink 기동 완료 ===");
        System.out.println("Kafka bootstrap servers : " + bootstrapServers);
        System.out.println("TimescaleDB             : " + jdbcUrl);
        System.out.println("factory.linestate -> line_state 하이퍼테이블로 batch insert");
        System.out.println("Prometheus 메트릭       : http://localhost:" + metricsPort + "/metrics");
        System.out.println("종료하려면 Ctrl+C 를 누르세요.");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> consumer.wakeup()));

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                if (records.isEmpty()) {
                    continue;
                }
                try {
                    for (ConsumerRecord<String, String> record : records) {
                        bind(db.insert, record.value());
                        db.insert.addBatch();
                    }
                    db.insert.executeBatch();
                    db.connection.commit();
                    INSERT_BATCHES_TOTAL.labels("success").inc();
                    RECORDS_INSERTED_TOTAL.inc(records.count());
                    System.out.println("[DB sink] " + records.count() + "건 적재");
                } catch (SQLException e) {
                    // 배치 하나가 통째로 실패했다는 건 DB 연결 자체가 끊겼을 가능성이 높다(예: 컨테이너
                    // 재시작). 여기서 죽어버리면 재기동 전까지 계속 데이터가 유실되므로, 연결을 새로
                    // 열어서 폴링 루프를 계속 이어간다 - 실패한 배치의 레코드는 유실된다(재처리 안 함).
                    INSERT_BATCHES_TOTAL.labels("failure").inc();
                    DB_CONNECTED.set(0);
                    System.err.println("[DB sink] 적재 실패, 재연결을 시도합니다: " + e.getMessage());
                    db.close();
                    db = reconnectWithBackoff(jdbcUrl, dbUser, dbPassword);
                }
            }
        } catch (WakeupException ignored) {
            // 종료 신호로 poll()이 깨어난 것 - 정상 흐름
        } finally {
            db.close();
            consumer.close();
            metricsServer.stop();
        }
    }

    /** DB가 계속 죽어있는 동안에도 프로세스가 내려가지 않도록, 지수 백오프로 재연결을 반복 시도한다. */
    private static DbHandle reconnectWithBackoff(String jdbcUrl, String dbUser, String dbPassword) {
        long delayMs = 1000;
        while (true) {
            try {
                Thread.sleep(delayMs);
                DbHandle handle = openConnection(jdbcUrl, dbUser, dbPassword);
                RECONNECT_TOTAL.inc();
                System.out.println("[DB sink] 재연결 성공");
                return handle;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                System.err.println("[DB sink] 재연결 실패, " + (delayMs / 1000) + "초 후 재시도: " + e.getMessage());
                delayMs = Math.min(delayMs * 2, 30_000);
            }
        }
    }

    /** JDBC 커넥션 + 그 커넥션에 종속된 PreparedStatement를 한 쌍으로 묶어 재연결 시 통째로 교체한다. */
    private static final class DbHandle {
        final Connection connection;
        final PreparedStatement insert;

        DbHandle(Connection connection, PreparedStatement insert) {
            this.connection = connection;
            this.insert = insert;
        }

        void close() {
            try {
                insert.close();
            } catch (SQLException ignored) {
                // 이미 끊긴 연결일 수 있으므로 종료 중 예외는 무시한다.
            }
            try {
                connection.close();
            } catch (SQLException ignored) {
                // 위와 동일한 이유로 무시한다.
            }
        }
    }

    /** 새 JDBC 커넥션을 열고 스키마를 보장한 뒤, insert용 PreparedStatement까지 준비해서 묶어 반환한다. */
    private static DbHandle openConnection(String jdbcUrl, String dbUser, String dbPassword) throws SQLException {
        Connection connection = DriverManager.getConnection(jdbcUrl, dbUser, dbPassword);
        connection.setAutoCommit(false);
        ensureSchema(connection);
        PreparedStatement insert = connection.prepareStatement(INSERT_SQL);
        DB_CONNECTED.set(1);
        return new DbHandle(connection, insert);
    }

    /** factory.linestate JSON 한 건을 INSERT 문의 파라미터로 바인딩한다. */
    private static void bind(PreparedStatement insert, String json) throws SQLException {
        String lineId = JsonFields.str(json, "lineId");
        double fireActual = JsonFields.num(json, "fireActual", 0);
        double moldActual = JsonFields.num(json, "moldActual", 0);
        double roomTemp = JsonFields.num(json, "roomTemp", 0);
        double roomHumidity = JsonFields.num(json, "roomHumidity", 0);
        int servedCount = (int) JsonFields.num(json, "servedCount", 0);
        long ts = (long) JsonFields.num(json, "ts", System.currentTimeMillis());

        insert.setTimestamp(1, new Timestamp(ts));
        insert.setString(2, lineId);
        insert.setDouble(3, fireActual);
        insert.setDouble(4, moldActual);
        insert.setDouble(5, roomTemp);
        insert.setDouble(6, roomHumidity);
        insert.setInt(7, servedCount);
    }

    /** line_state 하이퍼테이블이 없으면 만든다 (TimescaleDB 확장 활성화 포함). */
    private static void ensureSchema(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("CREATE EXTENSION IF NOT EXISTS timescaledb");
            statement.execute(
                    "CREATE TABLE IF NOT EXISTS line_state ("
                            + "ts TIMESTAMPTZ NOT NULL,"
                            + "line_id TEXT NOT NULL,"
                            + "fire_actual DOUBLE PRECISION,"
                            + "mold_actual DOUBLE PRECISION,"
                            + "room_temp DOUBLE PRECISION,"
                            + "room_humidity DOUBLE PRECISION,"
                            + "served_count INTEGER)");
            statement.execute("SELECT create_hypertable('line_state', 'ts', if_not_exists => true)");
        }
        connection.commit();
    }
}
