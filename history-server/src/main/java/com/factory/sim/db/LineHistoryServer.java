package com.factory.sim.db;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * {@code line_state} 하이퍼테이블에 쌓인 이력을 브라우저(Three.js의 live-monitor.html)가
 * 클릭 한 번으로 조회할 수 있게 열어주는 콜드 패스 전용 HTTP 엔드포인트.
 *
 * <p>{@link com.factory.sim.ws.LiveWebSocketServer}(핫 패스, DB를 거치지 않고 relay만 함)와
 * 완전히 분리된 별도 프로세스다 — "지금"은 WebSocket, "과거"는 이 엔드포인트, 라는 역할
 * 분담을 코드 구조로도 지킨다.</p>
 *
 * <p>{@code GET /history?lineId=line1&minutes=15} → 최근 N분치 JSON 배열
 * {@code [{"ts":..,"fireActual":..,"moldActual":..,"roomTemp":..,"roomHumidity":..,"servedCount":..}, ...]}
 * (오래된 것부터 순서대로).</p>
 *
 * <p>실행: {@code ./gradlew runLineHistoryServer}</p>
 */
public final class LineHistoryServer {

    public static void main(String[] args) throws Exception {
        System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out), true, StandardCharsets.UTF_8));
        System.setErr(new PrintStream(new FileOutputStream(FileDescriptor.err), true, StandardCharsets.UTF_8));

        int port = Integer.parseInt(System.getProperty("history.port", "8083"));
        String jdbcUrl = System.getProperty("db.url", "jdbc:postgresql://localhost:5432/factory");
        String dbUser = System.getProperty("db.user", "factory");
        String dbPassword = System.getProperty("db.password", "factory");

        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/history", exchange -> handleHistory(exchange, jdbcUrl, dbUser, dbPassword));
        server.setExecutor(null);
        server.start();

        System.out.println("=== LineHistoryServer 기동 완료 ===");
        System.out.println("포트        : " + port + " (GET /history?lineId=line1&minutes=15)");
        System.out.println("TimescaleDB : " + jdbcUrl);
        System.out.println("종료하려면 Ctrl+C 를 누르세요.");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("LineHistoryServer 종료 중...");
            server.stop(0);
        }));
    }

    private static void handleHistory(HttpExchange exchange, String jdbcUrl, String dbUser, String dbPassword)
            throws IOException {
        // live-monitor.html이 file:// 로 열려도 fetch가 막히지 않도록 전부 허용한다 (내부 도구용).
        exchange.getResponseHeaders().add("Access-Control-Allow-Origin", "*");
        exchange.getResponseHeaders().add("Content-Type", "application/json; charset=UTF-8");

        Map<String, String> query = parseQuery(exchange.getRequestURI());
        String lineId = query.get("lineId");
        int minutes;
        try {
            minutes = query.containsKey("minutes") ? Integer.parseInt(query.get("minutes")) : 15;
        } catch (NumberFormatException e) {
            minutes = 15;
        }

        if (lineId == null || lineId.isEmpty()) {
            sendResponse(exchange, 400, "{\"error\":\"lineId is required\"}");
            return;
        }

        try {
            String body = queryHistory(jdbcUrl, dbUser, dbPassword, lineId, minutes);
            sendResponse(exchange, 200, body);
        } catch (SQLException e) {
            sendResponse(exchange, 500, "{\"error\":\"" + e.getMessage().replace("\"", "'") + "\"}");
        }
    }

    private static String queryHistory(String jdbcUrl, String dbUser, String dbPassword,
                                        String lineId, int minutes) throws SQLException {
        String sql = "SELECT ts, fire_actual, mold_actual, room_temp, room_humidity, served_count "
                + "FROM line_state WHERE line_id = ? AND ts > now() - make_interval(mins => ?) "
                + "ORDER BY ts";

        StringBuilder json = new StringBuilder("[");
        try (Connection connection = DriverManager.getConnection(jdbcUrl, dbUser, dbPassword);
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, lineId);
            statement.setInt(2, minutes);
            try (ResultSet rs = statement.executeQuery()) {
                boolean first = true;
                while (rs.next()) {
                    if (!first) {
                        json.append(',');
                    }
                    first = false;
                    json.append(String.format(
                            Locale.US,
                            "{\"ts\":%d,\"fireActual\":%.1f,\"moldActual\":%.1f,"
                                    + "\"roomTemp\":%.1f,\"roomHumidity\":%.1f,\"servedCount\":%d}",
                            rs.getTimestamp("ts").getTime(),
                            rs.getDouble("fire_actual"),
                            rs.getDouble("mold_actual"),
                            rs.getDouble("room_temp"),
                            rs.getDouble("room_humidity"),
                            rs.getInt("served_count")));
                }
            }
        }
        json.append(']');
        return json.toString();
    }

    private static Map<String, String> parseQuery(URI uri) {
        Map<String, String> result = new HashMap<>();
        String query = uri.getRawQuery();
        if (query == null) {
            return result;
        }
        for (String pair : query.split("&")) {
            int eq = pair.indexOf('=');
            if (eq < 0) {
                continue;
            }
            String key = java.net.URLDecoder.decode(pair.substring(0, eq), StandardCharsets.UTF_8);
            String value = java.net.URLDecoder.decode(pair.substring(eq + 1), StandardCharsets.UTF_8);
            result.put(key, value);
        }
        return result;
    }

    private static void sendResponse(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }
}
