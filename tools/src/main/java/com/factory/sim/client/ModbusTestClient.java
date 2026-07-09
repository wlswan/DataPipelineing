package com.factory.sim.client;

import
        com.ghgande.j2mod.modbus.facade.ModbusTCPMaster;
import com.ghgande.j2mod.modbus.procimg.InputRegister;
import com.ghgande.j2mod.modbus.procimg.Register;
import com.ghgande.j2mod.modbus.procimg.SimpleRegister;

import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

/**
 * Modbus 테스트 클라이언트.
 *
 * <p>시뮬레이터({@code com.factory.sim.Main})가 먼저 떠 있어야 한다. 이 클래스는
 * j2mod의 "마스터(클라이언트)" API인 {@link ModbusTCPMaster}를 이용해서</p>
 * <ol>
 *   <li>Input Register(화력/몰드 실측온도, 생산개수)를 읽고</li>
 *   <li>Holding Register(화력 목표온도 SV)를 읽어보고</li>
 *   <li>그 값을 새로 써본 뒤(FC=0x06)</li>
 *   <li>다시 읽어서 실제로 반영됐는지 확인한다.</li>
 * </ol>
 *
 * <p>실행: {@code ./gradlew runModbusTestClient}</p>
 *
 * <p>지금은 시뮬레이터가 라인마다 독립된 Modbus 포트를 연다(라인 1 = 502, 라인 2 = 503, ...).
 * 다른 라인을 확인하려면 두 번째 인자로 포트를 바꿔서 실행하면 된다:
 * {@code ./gradlew runModbusTestClient --args="localhost 503"}</p>
 */
public final class ModbusTestClient {

    public static void main(String[] args) throws Exception {
        // 윈도우 콘솔에서 한글이 깨지는 것을 막기 위해 System.out을 UTF-8로 강제한다.
        System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out), true, StandardCharsets.UTF_8));

        String host = args.length > 0 ? args[0] : "localhost";
        int port = args.length > 1
                ? Integer.parseInt(args[1])
                : Integer.parseInt(System.getProperty("modbus.port", "502"));

        ModbusTCPMaster master = new ModbusTCPMaster(host, port);
        master.connect();
        try {
            System.out.println("[Modbus 테스트] 접속 성공: " + host + ":" + port);
            System.out.println();

            // ---- 1) Input Register 읽기 (FC=0x04) : addr 0~2 연속으로 3개 읽기 ----
            InputRegister[] inputRegs = master.readInputRegisters(0, 3);
            System.out.println("[Input Register 읽기]");
            System.out.println("  화력 실측온도 = " + (inputRegs[0].getValue() / 10.0) + " C");
            System.out.println("  몰드 실측온도 = " + (inputRegs[1].getValue() / 10.0) + " C");
            System.out.println("  생산개수 누적  = " + inputRegs[2].getValue());
            System.out.println();

            // ---- 2) Holding Register 읽기 (FC=0x03) : addr 0~1 연속으로 2개 읽기 ----
            Register[] holdingRegs = master.readMultipleRegisters(0, 2);
            System.out.println("[Holding Register 읽기 - 변경 전]");
            System.out.println("  화력 목표온도(SV) = " + (holdingRegs[0].getValue() / 10.0) + " C");
            System.out.println("  벨트속도 지령     = " + (holdingRegs[1].getValue() / 100.0) + " Hz");
            System.out.println();

            // ---- 3) 화력 목표온도(SV)를 200.0도로 변경 (FC=0x06, 단일 레지스터 쓰기) ----
            int newSetpointX10 = 2000; // 200.0도
            master.writeSingleRegister(0, new SimpleRegister(newSetpointX10));
            System.out.println("[Holding Register 쓰기] 화력 목표온도(SV)를 200.0 C 로 변경 요청 -> 완료");
            System.out.println();

            // ---- 4) 다시 읽어서 변경이 반영됐는지 확인 ----
            Register[] updated = master.readMultipleRegisters(0, 1);
            System.out.println("[Holding Register 읽기 - 변경 후]");
            System.out.println("  화력 목표온도(SV) = " + (updated[0].getValue() / 10.0) + " C");

            if (updated[0].getValue() == newSetpointX10) {
                System.out.println();
                System.out.println("테스트 성공: 쓰기 요청이 FactoryState에 정상 반영되었습니다.");
            } else {
                System.out.println();
                System.out.println("테스트 실패: 쓰기 요청이 반영되지 않았습니다.");
            }
        } finally {
            master.disconnect();
        }
    }
}
