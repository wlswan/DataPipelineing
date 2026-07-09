package com.factory.sim;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 붕어빵 제조라인의 "공유 상태(shared state)" 클래스.
 *
 * 이 프로그램 전체에서 물리적으로 의미 있는 값(온도, 개수 등)은 전부 이 클래스 안에만
 * 존재한다. 다른 클래스들은 이 객체 하나를 여러 스레드에서 함께 들여다보는 구조다.
 *
 * <p>값의 흐름은 방향이 정해져 있다.</p>
 * <pre>
 *   [제어값 SV]  Modbus 마스터(외부 SCADA) --쓰기--&gt; FactoryState --읽기--&gt; PhysicsSimulator
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
 * 읽고 쓰기 때문에, 모든 필드를 {@link AtomicInteger} / {@link AtomicReference}로 감싸서
 * synchronized 블록 없이도 스레드 안전(thread-safe)하게 만들었다.</p>
 */
public final class FactoryState {

    // ------------------------------------------------------------------
    // 1) 제어값 (SV, Set Value)
    //    Modbus Holding Register에 대한 쓰기(FC=0x06)로 값이 들어온다.
    //    -> PhysicsSimulator가 이 값을 "목표값"으로 삼아 읽기만 한다.
    // ------------------------------------------------------------------

    /** 화력 목표온도(SV), x10 스케일 (2200 = 220.0°C). */
    private final AtomicInteger fireSetpointX10 = new AtomicInteger(2200);

    /** 벨트속도 지령, x100 스케일 (140 = 1.40Hz). */
    private final AtomicInteger beltSpeedX100 = new AtomicInteger(140);

    // ------------------------------------------------------------------
    // 2) 센서/생산 실측값
    //    PhysicsSimulator(물리 시뮬레이션 스레드)만 값을 갱신한다.
    //    -> Modbus(Input Register), MQTT 모듈은 읽기만 한다.
    // ------------------------------------------------------------------

    /** 화력 실측온도, x10 스케일. */
    private final AtomicInteger fireActualX10 = new AtomicInteger(2200);

    /** 몰드(붕어빵 틀) 실측온도, x10 스케일. */
    private final AtomicInteger moldActualX10 = new AtomicInteger(2000);

    /** 누적 생산개수 (PLC 카운터 흉내). */
    private final AtomicInteger servedCount = new AtomicInteger(0);

    /**
     * 실내 온습도 (최신 IoT형 센서라고 가정한 값). temp/humidity 두 값을 한 묶음으로
     * "원자적으로" 교체하기 위해 불변 객체 {@link RoomEnv}를 AtomicReference에 담아 둔다.
     */
    private final AtomicReference<RoomEnv> roomEnv = new AtomicReference<>(new RoomEnv(22.0, 50.0));

    // ==================================================================
    // 제어값 getter / setter : Modbus Holding Register 쪽에서 public 접근
    // ==================================================================

    public int getFireSetpointX10() {
        return fireSetpointX10.get();
    }

    /** Modbus 마스터가 화력 목표온도(SV)를 새로 쓸 때 호출된다. */
    public void setFireSetpointX10(int value) {
        fireSetpointX10.set(value);
    }

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

    public int getFireActualX10() {
        return fireActualX10.get();
    }

    public int getMoldActualX10() {
        return moldActualX10.get();
    }

    public int getServedCount() {
        return servedCount.get();
    }

    public RoomEnv getRoomEnv() {
        return roomEnv.get();
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

    void setFireActualX10(int value) {
        fireActualX10.set(clampToShort(value));
    }

    void setMoldActualX10(int value) {
        moldActualX10.set(clampToShort(value));
    }

    void incrementServedCount() {
        servedCount.updateAndGet(current -> clampToShort(current + 1));
    }

    void setRoomEnv(double temp, double humidity) {
        roomEnv.set(new RoomEnv(temp, humidity));
    }

    /**
     * 실내 온습도를 담는 불변(immutable) 값 객체.
     * 두 값(temp, humidity)을 항상 "짝"으로 함께 교체해야 하기 때문에,
     * 필드 두 개를 따로 따로 AtomicInteger로 두지 않고 이렇게 하나로 묶었다.
     */
    public static final class RoomEnv {
        public final double temp;
        public final double humidity;

        public RoomEnv(double temp, double humidity) {
            this.temp = temp;
            this.humidity = humidity;
        }
    }
}
