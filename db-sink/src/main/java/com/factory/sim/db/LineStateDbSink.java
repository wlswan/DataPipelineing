package com.factory.sim.db;

import com.factory.sim.common.JsonFields;
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

    public static void main(String[] args) throws Exception {
        System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out), true, StandardCharsets.UTF_8));
        System.setErr(new PrintStream(new FileOutputStream(FileDescriptor.err), true, StandardCharsets.UTF_8));

        String bootstrapServers = System.getProperty("kafka.bootstrapServers", "localhost:9094");
        String jdbcUrl = System.getProperty("db.url", "jdbc:postgresql://localhost:5432/factory");
        String dbUser = System.getProperty("db.user", "factory");
        String dbPassword = System.getProperty("db.password", "factory");

        Connection connection = DriverManager.getConnection(jdbcUrl, dbUser, dbPassword);
        connection.setAutoCommit(false);
        ensureSchema(connection);
        PreparedStatement insert = connection.prepareStatement(INSERT_SQL);

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
        System.out.println("종료하려면 Ctrl+C 를 누르세요.");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> consumer.wakeup()));

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                if (records.isEmpty()) {
                    continue;
                }
                for (ConsumerRecord<String, String> record : records) {
                    bind(insert, record.value());
                    insert.addBatch();
                }
                insert.executeBatch();
                connection.commit();
                System.out.println("[DB sink] " + records.count() + "건 적재");
            }
        } catch (WakeupException ignored) {
            // 종료 신호로 poll()이 깨어난 것 - 정상 흐름
        } finally {
            insert.close();
            connection.close();
            consumer.close();
        }
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
