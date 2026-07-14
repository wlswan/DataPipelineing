package com.factory.sim;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * 붕어빵 제조라인의 "공유 상태(shared state)" 클래스.
 *
 * 이 프로그램 전체에서 물리적으로 의미 있는 값은 전부 이 클래스 안에만 존재한다.
 * 다른 클래스들은 이 객체 하나를 여러 스레드에서 함께 들여다보는 구조다.
 *
 * <p>값의 흐름은 방향이 정해져 있다.</p>
 * <pre>
 *   [제어값]     Modbus 마스터(외부 SCADA) --쓰기--&gt; FactoryState --읽기--&gt; (제어 대상)
 *   [센서값]     PhysicsSimulator          --쓰기--&gt; FactoryState --읽기--&gt; Modbus / MQTT
 * </pre>
 *
 * <p>즉, "물리 시뮬레이션이 계산한 센서값"은 PhysicsSimulator 딱 한 곳에서만 쓰기(write)가
 * 일어나고, Modbus 서버와 MQTT 퍼블리셔는 그 값을 읽기(read)만 한다. 이를 코드로도
 * 강제하기 위해 센서값 setter는 패키지 전용(package-private)으로 감춰서, 같은 패키지에 있는
 * PhysicsSimulator만 호출할 수 있게 만들었다 (Modbus/MQTT 클래스는 하위 패키지에 있어서
 * 아예 접근이 안 된다).</p>
 *
 * <p>여러 스레드(물리 스레드, Modbus 접속 스레드들, MQTT 발행 스레드)가 동시에 같은 값을
 * 읽고 쓰기 때문에, 모든 필드를 {@link AtomicInteger}로 감싸서
 * synchronized 블록 없이도 스레드 안전(thread-safe)하게 만들었다.</p>
 *
 * <p>실내온습도는 라인마다 있는 값이 아니라 공장 전체가 공유하는 값이라서 이 클래스가 아니라
 * {@link RoomEnvironment}가 따로 들고 있다.</p>
 */
public final class FactoryState {

    // ------------------------------------------------------------------
    // 1) 제어값
    //    Modbus Holding Register에 대한 쓰기(FC=0x06)로 값이 들어온다.
    // ------------------------------------------------------------------

    /** 벨트속도 지령, x100 스케일 (140 = 1.40Hz). */
    private final AtomicInteger beltSpeedX100 = new AtomicInteger(140);

    // ------------------------------------------------------------------
    // 2) 센서 실측값
    //    PhysicsSimulator(물리 시뮬레이션 스레드)만 값을 갱신한다.
    //    -> Modbus(Input Register), MQTT 모듈은 읽기만 한다.
    // ------------------------------------------------------------------

    /** 적외선 온도계로 측정한 실측온도, x10 스케일. */
    private final AtomicInteger irTempX10 = new AtomicInteger(2000);

    // ==================================================================
    // 제어값 getter / setter : Modbus Holding Register 쪽에서 public 접근
    // ==================================================================

    public int getBeltSpeedX100() {
        return beltSpeedX100.get();
    }

    /** Modbus 마스터가 벨트속도 지령을 새로 쓸 때 호출된다. */
    public void setBeltSpeedX100(int value) {
        beltSpeedX100.set(value);
    }

    // ==================================================================
    // 센서값 getter : 누구나(Modbus, MQTT) 읽을 수 있음
    // ==================================================================

    public int getIrTempX10() {
        return irTempX10.get();
    }

    // ==================================================================
    // 센서값 setter : 패키지 전용(package-private) -> PhysicsSimulator만 호출 가능
    // ==================================================================

    /** 16비트 부호있는 정수(short) 범위(-32768~32767)를 벗어나지 않도록 자른다. */
    private static int clampToShort(int value) {
        if (value > Short.MAX_VALUE) {
            return Short.MAX_VALUE;
        }
        if (value < Short.MIN_VALUE) {
            return Short.MIN_VALUE;
        }
        return value;
    }

    void setIrTempX10(int value) {
        irTempX10.set(clampToShort(value));
    }
}
