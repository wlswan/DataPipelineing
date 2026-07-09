package com.factory.sim.modbus;

import com.factory.sim.FactoryState;
import com.ghgande.j2mod.modbus.procimg.SimpleProcessImage;
import com.ghgande.j2mod.modbus.slave.ModbusSlave;
import com.ghgande.j2mod.modbus.slave.ModbusSlaveFactory;

/**
 * Modbus TCP 서버(슬레이브) 모듈.
 *
 * <p>이 클래스가 여는 접속(TCP 소켓 accept)과 요청 처리는 j2mod 라이브러리가 내부적으로
 * 자기만의 스레드 풀에서 처리한다. 우리가 할 일은 "어떤 주소(address)에 어떤 값을 연결할지"
 * 레지스터 맵을 만들어서 j2mod에게 넘겨주는 것뿐이다.</p>
 *
 * <h2>레지스터 맵</h2>
 * <pre>
 * Input Register (FC=0x04, 읽기전용 - "직접형 센서")
 *   addr 0 : 화력 실측온도, x10 스케일 (2200 = 220.0°C)
 *   addr 1 : 몰드 실측온도, x10 스케일
 *   addr 2 : 생산개수 누적
 *
 * Holding Register (FC=0x03 읽기 / FC=0x06 쓰기)
 *   addr 0 : 화력 목표온도(SV), x10 스케일
 *   addr 1 : 벨트속도 지령, x100 스케일 (140 = 1.40Hz)
 * </pre>
 *
 * <p>Input Register 쪽에는 {@link DelegatingInputRegister}만 연결했기 때문에 애초에
 * "쓰기(setValue)" 자체가 불가능하다 - 즉 Modbus 마스터는 센서값을 절대 조작할 수 없고
 * 읽기만 할 수 있다. 반대로 Holding Register는 SV/벨트속도라는 "제어값"이라서
 * 읽기/쓰기 둘 다 열어뒀다.</p>
 */
public final class ModbusServer {

    /** Modbus 슬레이브(장비) 식별 번호. 하나짜리 라인이라 1로 고정. */
    private static final int UNIT_ID = 1;

    /** 동시 접속을 처리할 j2mod 내부 스레드풀 크기. */
    private static final int LISTENER_THREAD_POOL_SIZE = 5;

    private final int port;
    private final FactoryState state;
    private ModbusSlave slave;

    public ModbusServer(int port, FactoryState state) {
        this.port = port;
        this.state = state;
    }

    /** Modbus TCP 서버를 시작한다. 호출 즉시 리턴하고, 이후 접속 처리는 j2mod의 내부 스레드가 담당한다. */
    public void start() throws Exception {
        SimpleProcessImage processImage = new SimpleProcessImage(UNIT_ID);

        // ---- Input Register (FC=0x04, 읽기전용) : FactoryState의 센서값을 그대로 연결 ----
        processImage.addInputRegister(0, new DelegatingInputRegister(state::getFireActualX10));
        processImage.addInputRegister(1, new DelegatingInputRegister(state::getMoldActualX10));
        processImage.addInputRegister(2, new DelegatingInputRegister(state::getServedCount));

        // ---- Holding Register (FC=0x03 읽기 / FC=0x06 쓰기) : FactoryState의 제어값을 연결 ----
        processImage.addRegister(0, new DelegatingRegister(state::getFireSetpointX10, state::setFireSetpointX10));
        processImage.addRegister(1, new DelegatingRegister(state::getBeltSpeedX100, state::setBeltSpeedX100));

        slave = ModbusSlaveFactory.createTCPSlave(port, LISTENER_THREAD_POOL_SIZE);
        slave.addProcessImage(UNIT_ID, processImage);
        slave.open();

        System.out.println("[Modbus] TCP 서버 시작 완료 (port=" + port + ", unitId=" + UNIT_ID + ")");
    }

    /** Modbus TCP 서버를 종료한다. */
    public void stop() {
        if (slave != null) {
            slave.close();
        }
    }
}
