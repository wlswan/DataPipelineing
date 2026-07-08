package com.factory.sim;

import com.factory.sim.modbus.ModbusServer;
import com.factory.sim.mqtt.EmbeddedBroker;
import com.factory.sim.mqtt.RoomEnvPublisher;

import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 프로그램 진입점.
 *
 * <p>하나의 JVM 프로세스 안에서 "라인(생산 라인)" 여러 개를 동시에 흉내낸다. 라인 하나는
 * 최신 PLC 한 대가 자체 이더넷 포트를 갖는 구성을 흉내낸 것이라서, 라인마다 독립된
 * {@link PhysicsSimulator}(물리 계산 스레드)와 독립된 {@link ModbusServer}(자기만의 TCP
 * 포트)를 갖는다. 반면 MQTT는 브로커 하나를 모든 라인이 공유하고, 라인마다 토픽만
 * 다르게 써서 구분한다({@code factory/line{n}/roomEnv}) — 실제로도 IoT 센서들은 보통
 * 브로커 하나에 다 같이 붙고 토픽으로 구분되는 게 일반적이기 때문이다.</p>
 *
 * <p>라인 하나 안에서 뜨는 세 모듈({@link PhysicsSimulator}, {@link ModbusServer},
 * {@link RoomEnvPublisher})은 그 라인의 {@link FactoryState} 인스턴스 하나를 공유한다.
 * 셋 중 PhysicsSimulator만 값을 쓰고(write), 나머지 둘은 읽기(read)만 한다는 점이 이
 * 프로젝트의 핵심 설계다.</p>
 */
public final class Main {

    /** 라인 하나가 띄우는 리소스 묶음. */
    private record LineRuntime(
            int lineNumber,
            FactoryState state,
            PhysicsSimulator simulator,
            ModbusServer modbusServer,
            RoomEnvPublisher publisher
    ) {
    }

    public static void main(String[] args) throws Exception {
        // 윈도우(특히 한국어 로캘)에서는 JVM 기본 콘솔 인코딩이 CP949라서, -Dfile.encoding=UTF-8
        // 옵션만으로는 System.out의 한글 출력이 깨지는 경우가 있다. 그래서 System.out/err 자체를
        // UTF-8로 인코딩하는 PrintStream으로 아예 교체해서, 콘솔 인코딩 설정과 무관하게
        // 항상 올바른 UTF-8 바이트가 나가도록 만든다.
        System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out), true, StandardCharsets.UTF_8));
        System.setErr(new PrintStream(new FileOutputStream(FileDescriptor.err), true, StandardCharsets.UTF_8));

        // 라인 개수와 포트는 System property로 바꿀 수 있게 했다.
        // 예: ./gradlew run -Dlines=7 -Dmodbus.basePort=502 -Dmqtt.port=1883
        // (리눅스/macOS에서는 502, 1883처럼 1024 미만 포트를 쓰려면 관리자 권한이 필요할 수 있다.
        //  윈도우에서는 보통 권한 없이도 열리지만, 막히면 위 옵션으로 다른 포트를 지정하면 된다.)
        int lineCount = Integer.parseInt(System.getProperty("lines", "7"));
        int modbusBasePort = Integer.parseInt(System.getProperty("modbus.basePort", "502"));
        int mqttPort = Integer.parseInt(System.getProperty("mqtt.port", "1883"));

        // 1) MQTT 임베디드 브로커는 모든 라인이 공유하므로 먼저 하나만 띄운다.
        EmbeddedBroker broker = new EmbeddedBroker(mqttPort);
        broker.start();

        // 2) 라인마다 물리 시뮬레이션 스레드 + Modbus TCP 서버(자기만의 포트) + MQTT 퍼블리셔를 띄운다.
        //    라인 n의 Modbus 포트는 modbusBasePort + (n - 1) 이다 (예: 502, 503, 504, ...).
        List<LineRuntime> lines = new ArrayList<>(lineCount);
        for (int lineNumber = 1; lineNumber <= lineCount; lineNumber++) {
            int modbusPort = modbusBasePort + (lineNumber - 1);

            FactoryState state = new FactoryState();

            PhysicsSimulator simulator = new PhysicsSimulator(state);
            simulator.start();

            ModbusServer modbusServer = new ModbusServer(modbusPort, state);
            modbusServer.start();

            RoomEnvPublisher publisher = new RoomEnvPublisher("tcp://localhost:" + mqttPort, state, lineNumber);
            publisher.start();

            lines.add(new LineRuntime(lineNumber, state, simulator, modbusServer, publisher));
        }

        System.out.println("=== 붕어빵 공장 시뮬레이터 기동 완료 (" + lineCount + "개 라인) ===");
        System.out.println("MQTT 브로커 : tcp://localhost:" + mqttPort + " (모든 라인 공유)");
        System.out.println();
        System.out.println("라인 | Modbus TCP           | MQTT 토픽");
        System.out.println("-----|----------------------|--------------------------");
        for (LineRuntime line : lines) {
            int modbusPort = modbusBasePort + (line.lineNumber() - 1);
            System.out.printf(
                    "%4d | localhost:%-10d | factory/line%d/roomEnv%n",
                    line.lineNumber(), modbusPort, line.lineNumber());
        }
        System.out.println();
        System.out.println("종료하려면 Ctrl+C 를 누르세요.");

        // Ctrl+C 등으로 프로세스가 종료될 때, 각 라인을 역순으로 정리한 뒤 공유 브로커를 내린다.
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("시뮬레이터 종료 중...");
            List<LineRuntime> reversed = new ArrayList<>(lines);
            Collections.reverse(reversed);
            for (LineRuntime line : reversed) {
                line.modbusServer().stop();
                line.publisher().stop();
                line.simulator().stop();
            }
            broker.stop();
        }));

        // main 스레드는 여기서 그냥 대기한다. 실제 동작은 위에서 띄운
        // 백그라운드 스레드(물리 시뮬레이션, Modbus 리스너, MQTT 발행)들이 전부 담당한다.
        Thread.currentThread().join();
    }
}
