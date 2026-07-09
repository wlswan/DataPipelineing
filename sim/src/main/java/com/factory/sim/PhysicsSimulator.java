package com.factory.sim;

import java.util.Random;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 물리 시뮬레이션 스레드.
 *
 * <p>자바에서 "일정 주기로 반복 실행"을 하고 싶을 때 가장 쉬운 방법은
 * {@link ScheduledExecutorService}를 쓰는 것이다. {@code new Thread()} + {@code while(true) + sleep()}
 * 로 직접 짜는 것보다 예외 처리, 종료(shutdown) 처리가 훨씬 간단하다.</p>
 *
 * <p>이 클래스는 {@link FactoryState}의 "센서값"을 갱신하는 유일한 주체다. 다른 어떤 클래스도
 * 센서값을 직접 쓰지 않는다 (setter가 패키지 전용이라 애초에 불가능하다). 이렇게 "한 곳에서만
 * 쓰기가 일어난다"는 규칙을 지키면, 여러 스레드가 얽혀도 값이 꼬일 걱정을 크게 줄일 수 있다.</p>
 */
public final class PhysicsSimulator implements Runnable {

    /** 물리 계산 주기 (초). 문제에서 요구한 "0.5초마다 갱신"에 대응. */
    private static final double DT_SECONDS = 0.5;

    /** 화력 실측온도가 목표온도(SV)를 따라가는 속도를 결정하는 시정수 (초). 작을수록 빨리 반응. */
    private static final double FIRE_TIME_CONSTANT = 3.0;

    /** 몰드온도가 목표 몰드온도를 따라가는 시정수 (초). 화력보다 열용량이 커서 더 느리게 반응. */
    private static final double MOLD_TIME_CONSTANT = 8.0;

    /** 몰드온도가 이 값(°C) 이상일 때만 붕어빵이 "익었다"고 보고 생산 카운트가 오를 수 있다. */
    private static final double MOLD_READY_TEMP = 180.0;

    /** 몰드온도가 충분히 높을 때, tick(0.5초)마다 생산개수가 1개 늘어날 확률. */
    private static final double PRODUCE_PROBABILITY_PER_TICK = 0.03;

    /** 센서 오차(노이즈)의 최대 폭. 문제에서 요구한 "±0.3 정도"에 대응. */
    private static final double SENSOR_NOISE_RANGE = 0.3;

    private final FactoryState state;
    private final Random random = new Random();

    /**
     * 물리 계산 전용 스레드를 만들어주는 실행기(executor).
     * daemon 스레드로 만들어서, 메인 스레드가 끝나면 JVM이 이 스레드 때문에 안 죽고 잘 종료되게 한다.
     */
    private final ScheduledExecutorService executor = new ScheduledThreadPoolExecutor(1, runnable -> {
        Thread thread = new Thread(runnable, "physics-sim");
        thread.setDaemon(true);
        return thread;
    });

    // ---- 물리 계산용 내부 상태 (double로 고정밀 유지) ----
    // FactoryState에는 x10 스케일 정수(클램핑됨)로만 내보내고, 계산 자체는 이 필드들로 한다.
    private double actFireTemp;    // 화력 실측온도 (°C)
    private double moldTemp;       // 몰드 실측온도 (°C)
    private double roomTemp = 22.0;      // 실내온도 (°C)
    private double roomHumidity = 50.0;  // 실내습도 (%RH)

    public PhysicsSimulator(FactoryState state) {
        this.state = state;
        // FactoryState의 초기값과 어긋나지 않도록 내부 상태도 같은 값에서 시작한다.
        this.actFireTemp = state.getFireActualX10() / 10.0;
        this.moldTemp = state.getMoldActualX10() / 10.0;
    }

    /** 0.5초 간격으로 {@link #run()}을 반복 호출하도록 예약한다. */
    public void start() {
        long periodMillis = (long) (DT_SECONDS * 1000);
        executor.scheduleAtFixedRate(this, 0, periodMillis, TimeUnit.MILLISECONDS);
    }

    /** 시뮬레이션 스레드를 정지시킨다. */
    public void stop() {
        executor.shutdownNow();
    }

    @Override
    public void run() {
        try {
            tick();
        } catch (RuntimeException e) {
            // scheduleAtFixedRate는 실행 중 예외가 던져지면 이후 반복 자체를 멈춰버린다.
            // 시뮬레이터가 조용히 멈추는 것보다는, 에러를 로그로 남기고 다음 tick도 계속 도는 게 낫다.
            e.printStackTrace();
        }
    }

    /** 한 번의 물리 계산 스텝. 0.5초마다 이 메서드가 호출된다. */
    private void tick() {
        // 1) 제어값(SV) 읽기 - Modbus 마스터가 홀딩 레지스터에 써 넣은 목표값
        double fireSetpoint = state.getFireSetpointX10() / 10.0;

        // 2) 화력 실측온도가 목표온도를 향해 "시정수 3초"짜리 1차 지연으로 서서히 수렴한다.
        //    공식: actFireTemp += (setpoint - actFireTemp) * min(1.0, dt/시정수)
        actFireTemp += (fireSetpoint - actFireTemp) * Math.min(1.0, DT_SECONDS / FIRE_TIME_CONSTANT);

        // 3) 몰드(틀)가 도달하려는 목표온도 = 화력온도 영향 90% + 실내온도 영향 10%
        double targetMold = actFireTemp * 0.90 + roomTemp * 0.10;

        // 4) 몰드 실측온도가 목표 몰드온도를 향해 "시정수 8초"로 서서히 수렴한다 (화력보다 느림).
        moldTemp += (targetMold - moldTemp) * Math.min(1.0, DT_SECONDS / MOLD_TIME_CONSTANT);

        // 5) 실내온습도는 사람이 문을 여닫거나 날씨가 바뀌는 정도의 "아주 느린 랜덤워크"로 흔들린다.
        roomTemp += (random.nextDouble() - 0.5) * 0.02;
        roomHumidity += (random.nextDouble() - 0.5) * 0.05;
        roomHumidity = Math.max(0.0, Math.min(100.0, roomHumidity));

        // 6) 실측값에는 센서 오차를 흉내낸 작은 랜덤 노이즈(±0.3)를 더한다.
        double fireNoisy = actFireTemp + (random.nextDouble() - 0.5) * (2 * SENSOR_NOISE_RANGE);
        double moldNoisy = moldTemp + (random.nextDouble() - 0.5) * (2 * SENSOR_NOISE_RANGE);

        // 7) 몰드온도가 충분히 뜨거울 때만, 낮은 확률로 생산개수가 1개 늘어난다 (PLC 카운터 흉내).
        //    실제 온도(moldTemp, 노이즈 제거 전 값)를 기준으로 판단해서 노이즈 때문에
        //    경계값에서 들쭉날쭉 카운트가 튀는 것을 막는다.
        if (moldTemp >= MOLD_READY_TEMP && random.nextDouble() < PRODUCE_PROBABILITY_PER_TICK) {
            state.incrementServedCount();
        }

        // 8) 계산 결과를 공유 상태(FactoryState)에 반영한다.
        //    x10 스케일 정수 변환과 16비트 범위 클램핑은 FactoryState 내부에서 처리한다.
        state.setFireActualX10((int) Math.round(fireNoisy * 10));
        state.setMoldActualX10((int) Math.round(moldNoisy * 10));
        state.setRoomEnv(roomTemp, roomHumidity);
    }
}
